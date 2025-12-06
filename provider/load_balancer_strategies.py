import asyncio
import math
import random
from typing import List, Optional, Dict, Any

from astrbot.core.provider.provider import Provider


class LoadBalanceStrategy:
    """负载均衡策略接口"""

    def __init__(self, provider_manager):
        self.provider_manager = provider_manager

    async def select_provider(self, providers: List[Provider], **kwargs) -> Optional[Provider]:
        """选择provider"""
        raise NotImplementedError


class ExplorationStrategy(LoadBalanceStrategy):
    """探索与利用平衡的负载均衡策略基类"""

    def __init__(self, provider_manager, exploration_factor=2.0):
        super().__init__(provider_manager)
        self.exploration_factor = exploration_factor

    def calculate_base_score(self, provider_stat: dict) -> float:
        """子类需要实现：计算基础评分"""
        raise NotImplementedError

    async def select_provider(self, providers: List[Provider], stats: Dict[str, Any] = None, **kwargs) -> Optional[
        Provider]:
        if not providers:
            return None

        if stats is None:
            stats = {}

        # 计算每个provider的综合评分（基础评分 + 基于选择次数的探索奖励）
        provider_scores = []
        total_score = 0

        for provider in providers:
            provider_id = provider.meta().id
            provider_stat = stats.get(provider_id, {"success": 0, "failure": 0})

            base_score = self.calculate_base_score(provider_stat)
            total_selections = provider_stat["success"] + provider_stat["failure"]

            # 计算探索奖励：选择次数越少，奖励越高
            if total_selections > 0:
                exploration_bonus = self.exploration_factor * math.sqrt(
                    math.log(total_selections + 1) / total_selections)
            else:
                # 从未被选择，给予最大探索奖励
                exploration_bonus = self.exploration_factor * 10

            # 综合评分 = 基础评分 + 探索奖励
            score = base_score + exploration_bonus
            provider_scores.append((provider, score))
            total_score += score

        # 加权随机选择
        if total_score > 0:
            random_score = random.uniform(0, total_score)
            current_score = 0

            for provider, score in provider_scores:
                current_score += score
                if random_score <= current_score:
                    return provider

        # 如果所有评分都为0，随机选择
        return random.choice(providers)


class RoundRobinStrategy(LoadBalanceStrategy):
    """轮询策略"""

    def __init__(self, provider_manager):
        super().__init__(provider_manager)
        self._lock = asyncio.Lock()
        self.current_index = 0

    async def select_provider(self, providers: List[Provider], **kwargs) -> Optional[Provider]:
        if not providers:
            return None
        async with self._lock:
            provider = providers[self.current_index % len(providers)]
            self.current_index = (self.current_index + 1) % len(providers)
        return provider


class RandomStrategy(LoadBalanceStrategy):
    """随机策略"""

    async def select_provider(self, providers: List[Provider], **kwargs) -> Optional[Provider]:
        if not providers:
            return None
        return random.choice(providers)


class WeightedStrategy(LoadBalanceStrategy):
    """加权策略"""

    def __init__(self, provider_manager):
        super().__init__(provider_manager)
        self.failback_strategy = RoundRobinStrategy(provider_manager)

    async def select_provider(self, providers: List[Provider], weights: Dict[str, Any] = None, **kwargs) -> Optional[
        Provider]:
        if not providers:
            return None

        if not weights:
            return await self.failback_strategy.select_provider(providers)

        # 构建权重区间
        weighted_list = []
        total_weight = 0

        for provider in providers:
            provider_weight_config = None
            provider_id = provider.meta().id
            for weight_config in weights.values():
                if isinstance(weight_config, dict) and weight_config.get("provider") == provider_id:
                    provider_weight_config = weight_config
                    break

            weight = 1  # 默认权重
            if provider_weight_config:
                try:
                    weight = int(provider_weight_config.get("weight", 1))
                except (ValueError, TypeError):
                    weight = 1

            if weight > 0:
                total_weight += weight
                weighted_list.append((provider, total_weight))

        if total_weight == 0:
            return await self.failback_strategy.select_provider(providers)

        # 随机选择一个权重点
        random_weight = random.randint(0, total_weight - 1)

        # 遍历查找对应的provider
        for provider, end_weight in weighted_list:
            if random_weight < end_weight:
                return provider

        # 理论上不应该到达这里，但作为兜底
        return await self.failback_strategy.select_provider(providers)


class FastestStrategy(ExplorationStrategy):
    """最快响应策略（基于吞吐量和选择次数）"""

    def calculate_base_score(self, provider_stat: dict) -> float:
        """基于吞吐量计算基础评分"""
        return provider_stat.get("throughput", 0)


class LeastFailureStrategy(ExplorationStrategy):
    """最少失败策略（结合成功率和选择次数）"""

    def calculate_base_score(self, provider_stat: dict) -> float:
        """基于成功率计算基础评分"""
        success_count = provider_stat.get("success", 0)
        failure_count = provider_stat.get("failure", 0)
        total_requests = success_count + failure_count

        if total_requests > 0:
            return success_count / total_requests
        else:
            # 未被选择过的provider，给予高成功率评分以鼓励探索
            return 1.0
