from backtest.strategies.price_momentum import PriceMomentumStrategy
from backtest.strategies.quality_value_recovery import QualityValueRecoveryStrategy

STRATEGY_REGISTRY = {
    "price-momentum": PriceMomentumStrategy(),
    "quality-value-recovery": QualityValueRecoveryStrategy(),
}


def get_backtest_strategy(strategy_name: str):
    try:
        return STRATEGY_REGISTRY[strategy_name]
    except KeyError as error:
        available = ", ".join(sorted(STRATEGY_REGISTRY))
        raise ValueError(
            f"未注册的回测策略: {strategy_name} (可选: {available})"
        ) from error


def list_backtest_strategies():
    return [STRATEGY_REGISTRY[name].metadata for name in sorted(STRATEGY_REGISTRY)]
