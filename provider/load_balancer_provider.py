import asyncio
from typing import List

from astrbot.api import logger
from astrbot.core import astrbot_config
from astrbot.core.agent.message import Message
from astrbot.core.provider.entities import LLMResponse, ToolCallsResult, ToolSet
from astrbot.core.provider.provider import Provider
from astrbot.core.provider.register import register_provider_adapter, ProviderType
from ..main import PROVIDER_MANAGER, LOAD_BALANCER_PROVIDER_TYPE_NAME
from .load_balance_service import LoadBalanceService

# 常量定义
DEFAULT_STRATEGY = "random"
DEFAULT_HEALTH_CHECK_INTERVAL = 30
WEIGHT_DEFAULT_VALUE = 1


@register_provider_adapter(
    provider_type_name=LOAD_BALANCER_PROVIDER_TYPE_NAME,
    desc="负载均衡Provider，支持多种负载均衡策略和失败回退机制",
    provider_type=ProviderType.CHAT_COMPLETION,
    provider_display_name="负载均衡Provider"
)
class LoadBalancerProvider(Provider):
    """
    负载均衡Provider，从ProviderManager获取除自己之外的其他Provider，并根据配置的策略分发请求。
    """

    def __init__(self, provider_config: dict, provider_settings: dict):
        super().__init__(provider_config, provider_settings)

        self.migrate(provider_config)
        # 从配置中获取负载均衡策略和设置
        self.strategy = provider_config.get("lb_strategy", DEFAULT_STRATEGY)  # 默认策略
        self.lb_node_list = provider_config.get("lb_node_list", [])
        # [(provider_id, weight)]
        self.provider_weights = [(n['provider'], n['weight']) for n in self.lb_node_list if n['__template_key'] == "node" and n['provider']]

        # 将健康检查间隔转换为整数
        health_check_interval = provider_config.get("lb_health_check_interval", str(DEFAULT_HEALTH_CHECK_INTERVAL))
        try:
            self.health_check_interval = int(health_check_interval)
        except (ValueError, TypeError):
            self.health_check_interval = DEFAULT_HEALTH_CHECK_INTERVAL  # 如果转换失败，使用默认值

        self.fallback_order = [p for p,w in self.provider_weights]

        # 这些将在initialize方法中设置
        self.work_providers = []
        self.health_check_task = None
        # 初始化负载均衡服务（包含所有状态管理）
        self.load_balance_service = LoadBalanceService(self)
        self._meta = super().meta()

    def migrate(self, provider_config: dict):
        lb_weights = provider_config.get("lb_weights", {})
        if not lb_weights:
            return
        lb_node_list = provider_config.get("lb_node_list", [])
        for k, node in sorted(lb_weights.items(),key=lambda x: int(x[0].split("_")[-1])):
            if node["provider"]:
                lb_node_list.append(
                    {
                        "__template_key": "node",
                        "provider": node["provider"],
                        "weight": node["weight"],
                    },
                )

        provider_config.update({"lb_node_list": lb_node_list})
        provider_config.pop("lb_weights")

        for p in astrbot_config['provider_sources']:
            if p["id"] == provider_config["provider_source_id"]:
                p.update({"lb_node_list": lb_node_list})
                p.pop("lb_weights")

        astrbot_config.save_config()


    def meta(self):
        if not self._meta:
            self._meta = super().meta()
        return self._meta

    def _load_other_providers(self):
        """加载除自己之外的其他Provider实例"""
        try:
            # 获取所有实例列表
            all_instances = PROVIDER_MANAGER.get_insts()

            # 过滤掉自己
            _providers = [
                provider for provider in all_instances
                if provider.meta().id != self.meta().id
            ]

            # 如果指定了fallback_order且当前策略不是weighted，则按照指定顺序重新排序
            # 加权策略使用权重配置而不是fallback_order作为排序依据
            if self.fallback_order and self.strategy != "weighted":
                # 使用字典快速索引所有providers
                provider_map = {provider.meta().id: provider for provider in _providers}
                ordered_providers = []

                # 提取字典中的provider_id值
                for provider_id in self.fallback_order:
                    if provider_id in provider_map:
                        ordered_providers.append(provider_map[provider_id])

                # 添加未在fallback_order中指定的provider
                fallback_set = set(self.fallback_order)
                for provider in _providers:
                    if provider.meta().id not in fallback_set:
                        ordered_providers.append(provider)
                _providers = ordered_providers

            self.work_providers.clear()
            self.work_providers.extend(_providers)
        except Exception as e:
            pass

    def get_current_key(self) -> str:
        """负载均衡器不使用具体的key，返回空字符串"""
        return ""

    def set_key(self, key: str):
        """负载均衡器不使用具体的key"""
        pass

    async def get_models(self) -> List[str]:
        return ["自动选择"]

    async def test(self, timeout: float = 45.0) -> None:
        logger.info("LoadBalancerProvider 无需测试")

    async def text_chat(
            self,
            prompt: str | None = None,
            session_id: str | None = None,
            image_urls: list[str] | None = None,
            func_tool: ToolSet | None = None,
            contexts: list[Message] | list[dict] | None = None,
            system_prompt: str | None = None,
            tool_calls_result: ToolCallsResult | list[ToolCallsResult] | None = None,
            model: str | None = None,
            **kwargs,
    ) -> LLMResponse:
        """通过负载均衡策略执行文本聊天"""
        # 确保已加载其他providers
        if not self.work_providers:
            self._load_other_providers()

        if not self.work_providers:
            raise Exception("没有可用的后端Provider")

        # 使用负载均衡选出provider并执行请求，包含故障转移逻辑
        return await self.load_balance_service.execute_with_load_balance_and_fallback(
            method_name="text_chat",
            prompt=prompt,
            session_id=session_id,
            image_urls=image_urls,
            func_tool=func_tool,
            contexts=contexts,
            system_prompt=system_prompt,
            tool_calls_result=tool_calls_result,
            model=None,
            **kwargs
        )

    async def text_chat_stream(
            self,
            prompt: str | None = None,
            session_id: str | None = None,
            image_urls: list[str] | None = None,
            func_tool: ToolSet | None = None,
            contexts: list[Message] | list[dict] | None = None,
            system_prompt: str | None = None,
            tool_calls_result: ToolCallsResult | list[ToolCallsResult] | None = None,
            model: str | None = None,
            **kwargs,
    ):
        """流式文本聊天"""
        # 确保已加载其他providers
        if not self.work_providers:
            self._load_other_providers()

        if not self.work_providers:
            raise Exception("没有可用的后端Provider")

        # 使用负载均衡选出provider并执行流式请求，包含故障转移逻辑
        async for chunk in self.load_balance_service.execute_with_load_balance_and_fallback_stream(
                method_name="text_chat_stream",
                prompt=prompt,
                session_id=session_id,
                image_urls=image_urls,
                func_tool=func_tool,
                contexts=contexts,
                system_prompt=system_prompt,
                tool_calls_result=tool_calls_result,
                model=None,
                **kwargs
        ):
            yield chunk

    async def initialize(self):
        """初始化方法，在这里可以进行额外的初始化工作"""
        # 启动健康检查任务
        if self.health_check_interval:
            self.health_check_task = asyncio.create_task(self._health_check_loop())

    async def _health_check_loop(self):
        """定期健康检查任务"""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self._perform_health_check()
            except Exception as e:
                pass

    async def _perform_health_check(self):
        """执行健康检查"""
        for provider in self.work_providers:
            logger.info(f"正在检查后端Provider {provider.meta().id} 的健康状态...")
            try:
                await provider.test()
                # 如果测试成功，可以重置失败统计
                self.load_balance_service.reset_failure_count(provider.meta().id)
            except Exception as e:
                # 测试失败，增加失败统计
                await self.load_balance_service.record_failure(provider.meta().id)

    async def terminate(self):
        """终止时的清理工作"""

        if hasattr(self, 'health_check_task') and self.health_check_task and not self.health_check_task.done():
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass

        await self.load_balance_service.terminate()

