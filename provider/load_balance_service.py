import asyncio
import inspect
from collections import defaultdict
from typing import List

from astrbot.api import logger
from astrbot.core.provider.entities import LLMResponse
from astrbot.core.provider.provider import Provider

from .load_balancer_strategies import (
    RoundRobinStrategy,
    RandomStrategy,
    WeightedStrategy,
    LeastFailureStrategy,
    FastestStrategy
)

FAILURE_RATE_THRESHOLD = 0.5
MAX_CONSECUTIVE_FAILURES = 3


class LoadBalanceService:
    """负载均衡服务，负责管理负载均衡和故障转移逻辑"""

    def __init__(self, provider_instance):
        self.provider = provider_instance
        self._other_providers = self.provider.work_providers
        self._strategy = self.provider.strategy
        self._fallback_order = self.provider.fallback_order
        self._provider_weights = self.provider.provider_weights

        self.strategies = {
            "round_robin": RoundRobinStrategy(provider_instance),
            "random": RandomStrategy(provider_instance),
            "weighted": WeightedStrategy(provider_instance),
            "least_failure": LeastFailureStrategy(provider_instance, exploration_factor=2.0),
            "fastest": FastestStrategy(provider_instance, exploration_factor=2.0)
        }

        # 状态管理
        self.provider_stats = defaultdict(lambda: {"success": 0, "failure": 0, "latency": 0.0, "throughput": 0.0})
        self.provider_health = {}
        # 统计更新队列
        self.stats_queue = asyncio.Queue()
        self.stats_task = asyncio.create_task(self.queue_consumer())

    def get_strategy(self, strategy_name: str):
        """获取指定策略"""
        return self.strategies.get(strategy_name, self.strategies["random"])  # 默认随机策略

    def _get_providers_with_order(self, providers: List[Provider], fallback_order: List[str]) -> List[Provider]:
        """根据fallback_order获取provider顺序"""
        provider_map = {provider.meta().id: provider for provider in providers}
        ordered_providers = []

        if fallback_order:
            for provider_id in fallback_order:
                if provider_id in provider_map:
                    ordered_providers.append(provider_map[provider_id])

        # 添加不在故障转移链中的provider
        # fallback_set = set(fallback_order) if fallback_order else set()
        # for provider in providers:
        #     if provider.meta().id not in fallback_set:
        #         ordered_providers.append(provider)

        return ordered_providers

    def _get_healthy_providers(self, providers: List[Provider]) -> List[Provider]:
        """获取健康provider列表"""
        healthy_providers = []
        for provider in providers:
            is_healthy = self.provider_health.get(provider.meta().id, True)
            if is_healthy:
                healthy_providers.append(provider)
            else:
                logger.debug(f"provider {provider.meta().id} 不健康，跳过")
        return healthy_providers

    async def _execute_with_load_balance_core(self, method_name: str, **kwargs):
        """
        通用的负载均衡核心执行函数，统一处理负载均衡和故障转移逻辑
        :param method_name: 要调用的方法名
        :param is_streaming: 是否是流式请求
        :param kwargs: 其他参数
        """

        available_providers = self._get_providers_with_order(self._other_providers, self._fallback_order)
        if not available_providers:
            raise RuntimeError("负载均衡器没有可用的provider")

        logger.debug(f"开始执行负载均衡，策略：{self._strategy}，"
                     f"可用provider：{[p.meta().id for p in available_providers]}")

        tried_count = 0
        while available_providers:
            tried_count += 1
            healthy_available = [p for p in available_providers if self.provider_health.get(p.meta().id, True)]
            _kwargs = {
                "stats": self.provider_stats,
                "weights": self._provider_weights,
            }
            selected_provider = await self.get_strategy(self._strategy).select_provider(healthy_available, **_kwargs)

            if not selected_provider:
                if available_providers:
                    selected_provider = available_providers[0]
                    logger.debug(f"策略无法选择provider，使用兜底选择: {selected_provider.meta().id}")
                else:
                    logger.debug(f"没有更多provider可尝试")
                    break
            logger.debug(f"第{tried_count}次[负载均衡|故障转移]选择Provider: {selected_provider.meta().id}")

            start_time = asyncio.get_event_loop().time()
            try:
                result = getattr(selected_provider, method_name)(**kwargs)
                tokens, size = 0, 0

                def _analyze(data):
                    nonlocal tokens, size
                    t, s = self.analyze_response(data)
                    tokens += t
                    size += s
                    return data

                try:
                    if inspect.isasyncgen(result):
                        async for chunk in result:
                            yield _analyze(chunk)
                    elif inspect.iscoroutine(result):
                        result = await result
                        yield _analyze(result)
                    else:
                        yield _analyze(result)
                except GeneratorExit:
                    pass

                elapsed = asyncio.get_event_loop().time() - start_time
                tokens = tokens if int(tokens) > 0 else size
                self.record_success(selected_provider.meta().id, elapsed, tokens)
                # logger.debug(f"provider {selected_provider.meta().id} 请求成功，耗时: {elapsed:.3f}秒，tokens: {tokens}")
                return
            except Exception as e:
                # logger.debug(f"provider {selected_provider.meta().id} 请求失败: {e}")
                self.record_failure(selected_provider.meta().id)

                # next Provider
                if selected_provider in available_providers:
                    available_providers.remove(selected_provider)

                if not available_providers:
                    logger.debug(f"所有provider都失败了，抛出异常")
                    raise e

    def analyze_response(self, resp: LLMResponse | None):
        if not resp or not isinstance(resp, LLMResponse):
            return 0, 0
        tokens = resp.raw_completion.usage.completion_tokens if resp.raw_completion else 0
        if resp.result_chain:
            size = sum([len(getattr(c, 'text')) for c in resp.result_chain.chain if getattr(c, 'text')])
        else:
            size = len(resp.completion_text or resp.reasoning_content or '')
        return tokens, size

    async def execute_with_load_balance_and_fallback(self, method_name: str, **kwargs) -> LLMResponse:
        """执行带负载均衡和故障转移的请求"""
        chunks = []
        async for chunk in self._execute_with_load_balance_core(method_name, **kwargs):
            chunks.append(chunk)
        return chunks[0] if chunks else None

    async def execute_with_load_balance_and_fallback_stream(self, method_name: str, **kwargs):
        """执行带负载均衡和故障转移的流式请求"""
        async for chunk in self._execute_with_load_balance_core(method_name, **kwargs):
            yield chunk

    async def _record_success(self, provider_id: str, latency: float, tokens: int):
        """记录成功请求"""
        self.provider_stats[provider_id]["success"] += 1

        # EWMA
        alpha = 0.5  # 平滑参数，值越接近1，对最新值越敏感

        # 指数加权移动平均: new_avg = alpha * new_value + (1 - alpha) * old_avg
        # 计算latency
        current_latency = self.provider_stats[provider_id]["latency"]
        new_latency = latency if current_latency == 0 else alpha * latency + (1 - alpha) * current_latency
        self.provider_stats[provider_id]["latency"] = new_latency

        # 计算throughput
        tp = tokens / latency if latency > 0 else 0
        current_tp = self.provider_stats[provider_id]["throughput"]
        new_tp = tp if current_tp == 0 else alpha * tp + (1 - alpha) * current_tp
        self.provider_stats[provider_id]["throughput"] = new_tp

        # 记录provider为健康状态
        self.provider_health[provider_id] = True
        logger.debug(
            f"记录成功: provider {provider_id}, "
            f"延迟: {latency:.3f}, "
            f"吞吐量: {self.provider_stats[provider_id]['throughput']:.3f} tokens/秒, "
            f"成功次数: {self.provider_stats[provider_id]['success']}"
        )

    async def _record_failure(self, provider_id: str):
        """记录失败请求"""
        self.provider_stats[provider_id]["failure"] += 1

        stats = self.provider_stats[provider_id]
        total = stats["success"] + stats["failure"]
        failure_rate = stats["failure"] / total if total > 0 else 0

        # 如果失败率超过阈值，标记为不健康
        if failure_rate > FAILURE_RATE_THRESHOLD:
            self.provider_health[provider_id] = False
            logger.debug(f"记录失败: provider {provider_id}, 失败率: {failure_rate:.2%}, 标记为不健康")
        # 或者如果连续失败超过阈值，也标记为不健康
        elif stats["failure"] >= MAX_CONSECUTIVE_FAILURES and stats["success"] == 0:
            self.provider_health[provider_id] = False
            logger.debug(f"记录失败: provider {provider_id}, 连续失败 {stats['failure']} 次, 标记为不健康")
        else:
            logger.debug(f"记录失败: provider {provider_id}, 失败次数: {stats['failure']}, 失败率: {failure_rate:.2%}")

    async def _reset_failure_count(self, provider_id: str):
        """重置失败计数（供健康检查调用）"""
        if provider_id in self.provider_stats:
            # 尝试减少失败次数以反映健康状态
            self.provider_stats[provider_id]["failure"] = max(0, self.provider_stats[provider_id]["failure"] - 1)

    def record_success(self, provider_id: str, latency: float, tokens: int):
        self.queue_product(self._record_success, provider_id, latency, tokens)

    def record_failure(self, provider_id: str):
        """记录失败请求（供外部调用）"""
        self.queue_product(self._record_failure, provider_id)

    def reset_failure_count(self, provider_id: str):
        self.queue_product(self._reset_failure_count, provider_id)

    def queue_product(self, func, *args, **kwargs):
        self.stats_queue.put_nowait((func, args, kwargs))

    async def queue_consumer(self):
        """处理统计更新的后台任务"""

        while True:
            try:
                # 等待统计更新请求
                func, args, kwargs = await self.stats_queue.get()
                try:
                    result = await func(*args, **kwargs)
                finally:
                    self.stats_queue.task_done()

            except asyncio.CancelledError:
                logger.debug("统计处理任务被取消")
                break
            except Exception as e:
                logger.error(f"处理统计更新时出错: {e}")
                continue

    async def terminate(self):
        """终止时的清理工作"""
        if hasattr(self, 'stats_task') and self.stats_task and not self.stats_task.done():
            self.stats_task.cancel()
            try:
                await self.stats_task
            except asyncio.CancelledError:
                pass

        # 等待所有队列项处理完成
        await self.stats_queue.join()
