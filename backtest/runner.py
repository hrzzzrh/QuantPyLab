import json
from collections import OrderedDict
from dataclasses import dataclass

import pandas as pd

from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.engine import (
    BacktestResult,
    DailyBacktestEngine,
    PreparedMarketData,
)
from backtest.strategy_base import validate_target_weights
from backtest.strategy_registry import get_backtest_strategy
from storage.database.manager import DBManager


def _select_execution_price_data(signal_data: pd.DataFrame) -> pd.DataFrame:
    """Keep only columns consumed by the daily execution engine."""

    required_columns = ["date", "symbol", "open", "open_hfq", "close_hfq"]
    if not set(required_columns).issubset(signal_data.columns):
        # Test doubles and third-party strategies may expose a smaller frame;
        # retain their original contract and let the engine report missing
        # execution columns if it is actually called.
        return signal_data
    return signal_data.loc[:, required_columns].copy()


@dataclass(frozen=True)
class BacktestRun:
    """一次已解析策略配置的内存回测结果。"""

    config: BacktestConfig
    targets: pd.DataFrame
    result: BacktestResult


class BacktestExecutionCache:
    """在单次研究评估窗口内复用因子策略的回测输入。"""

    def __init__(self, max_entries: int = 4, market_data_max_entries: int = 2):
        if isinstance(max_entries, bool) or not isinstance(max_entries, int):
            raise ValueError("max_entries 必须是正整数")
        if max_entries <= 0:
            raise ValueError("max_entries 必须是正整数")
        if isinstance(market_data_max_entries, bool) or not isinstance(
            market_data_max_entries, int
        ):
            raise ValueError("market_data_max_entries 必须是正整数")
        if market_data_max_entries <= 0:
            raise ValueError("market_data_max_entries 必须是正整数")
        self.max_entries = max_entries
        self.market_data_max_entries = market_data_max_entries
        self._factor_candidates: OrderedDict[
            tuple, tuple[pd.DataFrame, pd.DataFrame]
        ] = OrderedDict()
        self._market_data: OrderedDict[tuple, PreparedMarketData] = OrderedDict()
        self._cache_hits = 0
        self._cache_misses = 0
        self._market_cache_hits = 0
        self._market_cache_misses = 0

    @property
    def stats(self) -> dict[str, int]:
        """Return counters useful for tests and evaluation diagnostics."""

        return {
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
        }

    def clear(self) -> None:
        """Release cached data frames before moving to another evaluation window."""

        self._factor_candidates.clear()
        self._market_data.clear()

    @property
    def market_stats(self) -> dict[str, int]:
        """Return market-data cache counters useful for tests and diagnostics."""

        return {
            "cache_hits": self._market_cache_hits,
            "cache_misses": self._market_cache_misses,
        }

    def prepare_market_data(
        self, prices: pd.DataFrame, config: BacktestConfig
    ) -> PreparedMarketData:
        """Prepare interval-level price structures and reuse them across runs."""

        cache_key = (config.start_date, config.end_date)
        cached = self._market_data.get(cache_key)
        if cached is None:
            self._market_cache_misses += 1
            if len(self._market_data) >= self.market_data_max_entries:
                # The new prepared map is large; do not overlap it with the
                # previous interval when the cache is intentionally bounded.
                self._market_data.popitem(last=False)
            cached = DailyBacktestEngine.prepare_market_data(prices, config)
            self._market_data[cache_key] = cached
            self._market_data.move_to_end(cache_key)
            if len(self._market_data) > self.market_data_max_entries:
                self._market_data.popitem(last=False)
        else:
            self._market_cache_hits += 1
            self._market_data.move_to_end(cache_key)
        return cached

    def prepare_inputs(
        self,
        strategy,
        data_access: BacktestDataAccess,
        config: BacktestConfig,
        parameters: dict,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load signal data and build targets, reusing factor candidates when possible."""

        calculate_factor_frame = getattr(strategy, "calculate_factor_frame", None)
        prepare_target_candidates = getattr(strategy, "prepare_target_candidates", None)
        build_targets_from_candidates = getattr(
            strategy, "build_targets_from_candidates", None
        )
        if not all(
            callable(method)
            for method in (
                calculate_factor_frame,
                prepare_target_candidates,
                build_targets_from_candidates,
            )
        ):
            signal_data = strategy.load_signal_data(data_access, config, parameters)
            targets = strategy.build_targets(signal_data, config, parameters)
            return _select_execution_price_data(signal_data), targets

        cache_key = self._build_factor_cache_key(config, parameters, strategy)
        cached = self._factor_candidates.get(cache_key)
        if cached is None:
            self._cache_misses += 1
            if len(self._factor_candidates) >= self.max_entries:
                # Release the old interval before DuckDB starts materializing
                # the replacement; evicting only after the load briefly kept
                # both full signal frames alive at the memory peak.
                self._factor_candidates.popitem(last=False)
            signal_data = strategy.load_signal_data(data_access, config, parameters)
            factor_frame = calculate_factor_frame(signal_data, parameters)
            candidates = prepare_target_candidates(
                signal_data,
                factor_frame,
                config,
                parameters,
            )
            cached = (_select_execution_price_data(signal_data), candidates)
            self._factor_candidates[cache_key] = cached
            self._factor_candidates.move_to_end(cache_key)
            if len(self._factor_candidates) > self.max_entries:
                self._factor_candidates.popitem(last=False)
        else:
            self._cache_hits += 1
            self._factor_candidates.move_to_end(cache_key)

        signal_data, candidates = cached
        targets = build_targets_from_candidates(candidates, parameters)
        return signal_data, targets

    @staticmethod
    def _build_factor_cache_key(config, parameters, strategy) -> tuple:
        factor_weights = parameters["factor_weights"]
        factor_parameters = parameters["factor_parameters"]
        return (
            strategy.metadata.name,
            strategy.metadata.version,
            config.start_date,
            config.end_date,
            tuple(factor_weights),
            json.dumps(
                factor_parameters,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            ),
            parameters["min_listing_days"],
        )


def execute_backtest(
    config: BacktestConfig,
    database_manager: DBManager,
    *,
    include_benchmark: bool = True,
    execution_cache: BacktestExecutionCache | None = None,
) -> BacktestRun:
    """执行回测但不写结果文件，供 CLI 和研究评估器复用。"""

    strategy = get_backtest_strategy(config.strategy_name)
    parameters = strategy.validate_parameters(config.strategy_parameters)
    resolved_config = config.with_resolved_strategy(
        strategy.metadata.version, parameters
    )
    data_access = BacktestDataAccess(database_manager)
    prepared_market_data = None
    if execution_cache is None:
        signal_data = strategy.load_signal_data(
            data_access, resolved_config, parameters
        )
        targets = strategy.build_targets(signal_data, resolved_config, parameters)
        signal_data = _select_execution_price_data(signal_data)
    else:
        signal_data, targets = execution_cache.prepare_inputs(
            strategy,
            data_access,
            resolved_config,
            parameters,
        )
        prepared_market_data = execution_cache.prepare_market_data(
            signal_data, resolved_config
        )
    targets = validate_target_weights(targets)
    confirmed_delisting_dates = data_access.load_confirmed_delisting_dates(
        signal_data["symbol"].drop_duplicates().tolist(),
        resolved_config.end_date,
    )
    benchmark_prices = (
        data_access.load_benchmark_prices(resolved_config)
        if include_benchmark
        else None
    )
    engine = DailyBacktestEngine(resolved_config)
    if prepared_market_data is None:
        result = engine.run(
            signal_data,
            targets,
            benchmark_prices,
            confirmed_delisting_dates=confirmed_delisting_dates,
        )
    else:
        result = engine.run(
            signal_data,
            targets,
            benchmark_prices,
            prepared_market_data=prepared_market_data,
            confirmed_delisting_dates=confirmed_delisting_dates,
        )
    return BacktestRun(
        config=resolved_config,
        targets=targets,
        result=result,
    )
