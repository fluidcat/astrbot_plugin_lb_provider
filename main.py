from astrbot.api import logger
from astrbot.api.star import Context, Star
from astrbot.core import AstrBotConfig

# 硬编码provider类型名
LOAD_BALANCER_PROVIDER_TYPE_NAME = "load_balancer_chat_completion"
PROVIDER_MANAGER = None


class LBProviderPlugin(Star):

    def __init__(self, context: Context, config: AstrBotConfig ):
        super().__init__(context)
        self.context = context
        self.provider_manager = context.provider_manager
        self.original_load_provider = self.provider_manager.load_provider
        self.original_reload = self.provider_manager.reload
        self.original_terminate_provider = self.provider_manager.terminate_provider
        self.lb_provider_node_total = config.get("lb_provider_node_total", 5)
        global PROVIDER_MANAGER
        PROVIDER_MANAGER = context.provider_manager

    async def initialize(self):
        # 强制预清理：在导入适配器前，无条件删除既有注册，确保干净状态
        try:
            import astrbot.core.provider.register as _core_reg
            _map = getattr(_core_reg, "provider_cls_map", None)
            _list = getattr(_core_reg, "provider_registry", None)
            if _map is not None and (LOAD_BALANCER_PROVIDER_TYPE_NAME in _map):
                del _map[LOAD_BALANCER_PROVIDER_TYPE_NAME]
            for i in reversed(range(len(_list))):
                if _list[i].type == LOAD_BALANCER_PROVIDER_TYPE_NAME:
                    del _list[i]
            logger.debug(f"强制预清理：已移除 {LOAD_BALANCER_PROVIDER_TYPE_NAME} 既有注册。")
        except Exception:
            pass

        try:
            from .provider import load_balancer_provider
            self.update_lb_provider()
        except ImportError as e:
            logger.error(f"导入 {LOAD_BALANCER_PROVIDER_TYPE_NAME} 失败，请检查依赖是否安装: {e}")
            raise

    async def terminate(self):
        # 在插件终止时卸载相关的provider
        for p in self.context.get_all_providers():  # 获取所有chat completion providers
            pm = p.meta()
            if pm.type == LOAD_BALANCER_PROVIDER_TYPE_NAME:
                await self.context.provider_manager.terminate_provider(pm.id)

        # 删除动态注入的配置
        self.remove_dynamic_config()

    def update_lb_provider(self):
        self.inject_provider_metadata()
        self.check_provider_config()

    def check_provider_config(self):
        """更新已有的负载均衡Provider实例的配置"""
        lb_providers = []
        worker_provider_ids = set()
        for p in self.provider_manager.get_insts():
            if p.meta().type == LOAD_BALANCER_PROVIDER_TYPE_NAME:
                lb_providers.append(p)
            else:
                worker_provider_ids.add(p.meta().id)
        for p in lb_providers:
            weights = p.provider_config.get("lb_weights", {})
            for lb_key, provider_weight in list(weights.items()):
                provider_id = provider_weight.get("provider", "") if provider_weight else ""
                if provider_id not in worker_provider_ids:
                    provider_id and logger.warning(f"节点模型提供商【{provider_id}】未启用，该权重节点配置将被忽略")

    def inject_provider_metadata(self):
        """动态注入配置到CONFIG_METADATA_2"""
        try:
            from astrbot.core.config.default import CONFIG_METADATA_2

            weights_items = {}
            weights_tmpl = {}
            for index in range(0, self.lb_provider_node_total):
                seq = index + 1
                weights_tmpl[f"weight_node_{seq}"] = {"provider": "", "weight": "1"}
                weights_items[f"weight_node_{seq}"] = {
                    "description": f"权重节点",
                    "type": "object",
                    "items": {
                        "provider": {
                            "description": "节点模型",
                            "type": "string",
                            "_special": "select_provider",
                        },
                        "weight": {
                            "description": "权重",
                            "type": "string",
                            "hint": "正整数, 建议1~1000"
                        },
                    }
                }

            # 更新CONFIG_METADATA_2中的配置模板
            config_template = CONFIG_METADATA_2["provider_group"]["metadata"]["provider"]["config_template"]
            config_template["Load Balancer"] = {
                "id": "load_balancer_default",
                "type": LOAD_BALANCER_PROVIDER_TYPE_NAME,
                "provider": LOAD_BALANCER_PROVIDER_TYPE_NAME,
                "key": ["无需配置"],
                "api_base": "自动切换",
                "provider_type": "chat_completion",
                "enable": False,
                "lb_strategy": "round_robin",  # 负载均衡策略: round_robin, random, weighted, least_failure, fastest
                "lb_weights": weights_tmpl,  # 动态生成的权重配置
                "lb_health_check_interval": "1800"  # 健康检查间隔（秒）
            }

            # 更新项目配置
            CONFIG_METADATA_2["provider_group"]["metadata"]["provider"]["items"].update({
                "lb_strategy": {
                    "description": "负载均衡策略",
                    "type": "string",
                    "options": ["round_robin", "random", "weighted", "least_failure", "fastest"],
                    "hint": "轮询-round_robin, 随机-random, 加权-weighted, 最少故障-least_failure, 最快响应-fastest",
                },
                "lb_health_check_interval": {
                    "description": "健康检查间隔（秒）",
                    "type": "string",
                },
                "lb_weights": {
                    "description": "权重配置",
                    "type": "object",
                    "items": weights_items,
                    "hint": "负载节点顺序、权重配置，需要调整节点数量请通过插件配置调整。（权重值仅策略「weighted」时生效）",
                    "obvious_hint": True
                },
            })

            logger.debug(f"已为 {LOAD_BALANCER_PROVIDER_TYPE_NAME} 适配器注入动态配置")
        except Exception as expt:
            logger.debug(f"注入 {LOAD_BALANCER_PROVIDER_TYPE_NAME} 动态配置失败: {expt}")

    def remove_dynamic_config(self):
        """删除动态注入的配置"""
        try:
            from astrbot.core.config.default import CONFIG_METADATA_2

            # 从配置模板中移除负载均衡配置
            config_template = CONFIG_METADATA_2["provider_group"]["metadata"]["provider"]["config_template"]
            if "load_balancer" in config_template:
                del config_template["load_balancer"]

            # 从项目配置中移除相关配置
            items = CONFIG_METADATA_2["provider_group"]["metadata"]["provider"]["items"]
            keys_to_remove = ["lb_strategy", "lb_fallback_order", "lb_weights", "lb_health_check_interval"]
            for key in keys_to_remove:
                if key in items:
                    del items[key]

            logger.debug(f"已移除 {LOAD_BALANCER_PROVIDER_TYPE_NAME} 适配器的动态配置")
        except Exception as expt:
            logger.debug(f"移除 {LOAD_BALANCER_PROVIDER_TYPE_NAME} 动态配置失败: {expt}")
