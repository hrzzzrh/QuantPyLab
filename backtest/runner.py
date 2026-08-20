from dataclasses import dataclass

import pandas as pd

from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.engine import BacktestResult, DailyBacktestEngine
from backtest.strategy_base import validate_target_weights
from backtest.strategy_registry import get_backtest_strategy
from storage.database.manager import DBManager


@dataclass(frozen=True)
class BacktestRun:
    """一次已解析策略配置的内存回测结果。"""

    config: BacktestConfig
    targets: pd.DataFrame
    result: BacktestResult


def execute_backtest(
    config: BacktestConfig,
    database_manager: DBManager,
    *,
    include_benchmark: bool = True,
) -> BacktestRun:
    """执行回测但不写结果文件，供 CLI 和研究评估器复用。"""

    strategy = get_backtest_strategy(config.strategy_name)
    parameters = strategy.validate_parameters(config.strategy_parameters)
    resolved_config = config.with_resolved_strategy(
        strategy.metadata.version, parameters
    )
    data_access = BacktestDataAccess(database_manager)
    signal_data = strategy.load_signal_data(data_access, resolved_config, parameters)
    targets = validate_target_weights(
        strategy.build_targets(signal_data, resolved_config, parameters)
    )
    benchmark_prices = (
        data_access.load_benchmark_prices(resolved_config)
        if include_benchmark
        else None
    )
    result = DailyBacktestEngine(resolved_config).run(
        signal_data, targets, benchmark_prices
    )
    return BacktestRun(
        config=resolved_config,
        targets=targets,
        result=result,
    )
