import json
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

import main as main_module
from backtest.config import BacktestConfig
from backtest.liquidity_capacity_diagnostics import (
    LIQUIDITY_BUCKET_COLUMNS,
    LIQUIDITY_CAPACITY_SUMMARY_COLUMNS,
    LIQUIDITY_SIGNAL_SUMMARY_COLUMNS,
    LIQUIDITY_TRADE_COLUMNS,
    LiquidityCapacityReport,
    calculate_liquidity_buckets,
    calculate_liquidity_capacity_summary,
    calculate_liquidity_signal_summary,
    calculate_liquidity_trade_diagnostics,
    calculate_trailing_liquidity,
    write_factor_liquidity_capacity_report,
)


def _signal_data():
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    return pd.DataFrame(
        {
            "date": list(dates) + list(dates),
            "symbol": ["000001"] * 5 + ["000002"] * 5,
            "amount": [10.0, 20.0, 30.0, 40.0, 1000.0]
            + [100.0, 100.0, 100.0, 100.0, 100.0],
            "market_cap": [1000.0] * 5 + [2000.0] * 5,
        }
    )


def _snapshots():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03"] * 4),
            "symbol": ["000001", "000002", "000003", "000004"],
            "avg_amount": [1000.0, 2000.0, 3000.0, 4000.0],
            "market_cap": [10000.0, 20000.0, 30000.0, 40000.0],
            "amount_to_market_cap": [0.1, 0.1, 0.1, 0.1],
        }
    )


def _candidates_and_targets():
    candidates = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03"] * 4),
            "symbol": ["000001", "000002", "000003", "000004"],
        }
    )
    targets = candidates.iloc[[2, 3]].copy()
    return candidates, targets


def test_trailing_liquidity_uses_signal_date_and_prior_rows_only():
    snapshots = calculate_trailing_liquidity(_signal_data(), lookback_days=3)

    row = snapshots.loc[
        (snapshots["symbol"] == "000001")
        & (snapshots["date"] == pd.Timestamp("2024-01-04"))
    ].iloc[0]
    assert row["avg_amount"] == pytest.approx(30.0)
    assert snapshots.loc[
        (snapshots["symbol"] == "000001")
        & (snapshots["date"] == pd.Timestamp("2024-01-05")),
        "avg_amount",
    ].iloc[0] == pytest.approx((30.0 + 40.0 + 1000.0) / 3)
    assert (
        snapshots.loc[snapshots["date"] == pd.Timestamp("2024-01-01"), "avg_amount"]
        .isna()
        .all()
    )


def test_trailing_liquidity_can_release_non_signal_dates():
    snapshots = calculate_trailing_liquidity(
        _signal_data(),
        lookback_days=3,
        output_dates=[pd.Timestamp("2024-01-04")],
    )

    assert snapshots["date"].unique().tolist() == [pd.Timestamp("2024-01-04")]


def test_trailing_liquidity_rejects_duplicate_point_in_time_rows():
    data = pd.concat([_signal_data(), _signal_data().iloc[[0]]], ignore_index=True)

    with pytest.raises(ValueError, match="重复"):
        calculate_trailing_liquidity(data, lookback_days=3)


def test_liquidity_summary_and_buckets_keep_selected_coverage_auditable():
    candidates, targets = _candidates_and_targets()
    snapshots = _snapshots()
    summary = calculate_liquidity_signal_summary(
        candidates, targets, snapshots, bucket_count=2
    )
    buckets = calculate_liquidity_buckets(
        candidates, targets, snapshots, bucket_count=2
    )

    assert summary.loc[0, "candidate_count"] == 4
    assert summary.loc[0, "selected_count"] == 2
    assert summary.loc[0, "selected_amount_coverage_rate"] == pytest.approx(1.0)
    assert bool(summary.loc[0, "liquidity_bucket_eligible"])
    assert list(summary.columns) == list(LIQUIDITY_SIGNAL_SUMMARY_COLUMNS)
    assert buckets["liquidity_bucket"].tolist() == [1, 2]
    assert buckets["selected_count"].tolist() == [0, 2]
    assert list(buckets.columns) == list(LIQUIDITY_BUCKET_COLUMNS)


def test_liquidity_summary_rejects_target_outside_candidate_pool():
    candidates, targets = _candidates_and_targets()
    targets.loc[targets.index[0], "symbol"] = "999999"

    with pytest.raises(ValueError, match="必须属于"):
        calculate_liquidity_signal_summary(candidates, targets, _snapshots())


def test_trade_participation_and_capacity_use_signal_date_snapshot():
    trades = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-04", "2024-01-04", "2024-01-04"]),
            "signal_date": pd.to_datetime(["2024-01-03", "2024-01-03", None]),
            "symbol": ["000001", "000099", "000001"],
            "side": ["BUY", "SELL", "DELIST"],
            "raw_open": [1.0, 1.0, 1.0],
            "adjusted_open": [1.0, 1.0, 1.0],
            "notional": [100.0, 100.0, 50.0],
            "cost": [1.0, 1.0, 0.0],
            "reason": [
                "monthly_rebalance",
                "monthly_rebalance",
                "delisted_liquidation",
            ],
        }
    )
    diagnostic = calculate_liquidity_trade_diagnostics(
        trades,
        _snapshots(),
        initial_capital=1_000_000.0,
    )
    buy = diagnostic.loc[diagnostic["side"] == "BUY"].iloc[0]
    missing = diagnostic.loc[diagnostic["side"] == "SELL"].iloc[0]
    delist = diagnostic.loc[diagnostic["side"] == "DELIST"].iloc[0]

    assert buy["order_participation"] == pytest.approx(0.1)
    assert buy["capacity_capital_5pct"] == pytest.approx(500_000.0)
    assert missing["liquidity_status"] == "missing_signal_liquidity"
    assert pd.isna(missing["order_participation"])
    assert delist["liquidity_status"] == "delist_liquidation"
    assert list(diagnostic.columns) == list(LIQUIDITY_TRADE_COLUMNS)

    summary = calculate_liquidity_capacity_summary(diagnostic)
    assert summary.loc[0, "eligible_order_count"] == 1
    assert summary.loc[0, "missing_liquidity_order_count"] == 1
    assert summary.loc[0, "capacity_median"] == pytest.approx(500_000.0)
    assert list(summary.columns) == list(LIQUIDITY_CAPACITY_SUMMARY_COLUMNS)


def test_invalid_order_notional_is_not_counted_as_eligible_capacity_order():
    trades = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-04"]),
            "signal_date": pd.to_datetime(["2024-01-03"]),
            "symbol": ["000001"],
            "side": ["BUY"],
            "notional": [float("nan")],
        }
    )

    diagnostic = calculate_liquidity_trade_diagnostics(
        trades,
        _snapshots(),
        initial_capital=1_000_000.0,
    )

    assert diagnostic.loc[0, "liquidity_status"] == "invalid_order_notional"
    assert pd.isna(diagnostic.loc[0, "order_participation"])
    summary = calculate_liquidity_capacity_summary(diagnostic)
    assert summary.loc[0, "eligible_order_count"] == 0
    assert summary.loc[0, "invalid_order_notional_count"] == 1


def test_custom_participation_limit_creates_matching_capacity_column():
    trades = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-04"]),
            "signal_date": pd.to_datetime(["2024-01-03"]),
            "symbol": ["000001"],
            "side": ["BUY"],
            "notional": [100.0],
        }
    )

    diagnostic = calculate_liquidity_trade_diagnostics(
        trades,
        _snapshots(),
        initial_capital=1_000_000.0,
        participation_limits=(0.075,),
    )

    assert diagnostic.loc[0, "capacity_capital_7p5pct"] == pytest.approx(750_000.0)
    summary = calculate_liquidity_capacity_summary(
        diagnostic,
        participation_limits=(0.075,),
    )
    assert summary.loc[0, "participation_limit"] == pytest.approx(0.075)
    assert summary.loc[0, "capacity_median"] == pytest.approx(750_000.0)


def test_capacity_limits_are_validated():
    with pytest.raises(ValueError, match="不能重复"):
        calculate_liquidity_capacity_summary(
            pd.DataFrame(columns=LIQUIDITY_TRADE_COLUMNS),
            participation_limits=(0.05, 0.05),
        )


def test_liquidity_report_writes_standard_audit_files(tmp_path):
    empty_summary = pd.DataFrame(columns=LIQUIDITY_SIGNAL_SUMMARY_COLUMNS)
    empty_buckets = pd.DataFrame(columns=LIQUIDITY_BUCKET_COLUMNS)
    empty_trades = pd.DataFrame(columns=LIQUIDITY_TRADE_COLUMNS)
    empty_capacity = pd.DataFrame(columns=LIQUIDITY_CAPACITY_SUMMARY_COLUMNS)
    backtest_summary = pd.DataFrame(
        [
            {
                column: 0.0
                for column in (
                    "total_return",
                    "annualized_return",
                    "annualized_volatility",
                    "sharpe_ratio",
                    "max_drawdown",
                    "cumulative_turnover",
                    "total_transaction_cost",
                )
            }
            | {
                "trading_days": 1,
                "executed_trade_count": 0,
                "rebalance_day_count": 0,
                "skipped_rebalance_count": 0,
                "delist_count": 0,
            }
        ],
        columns=[
            "total_return",
            "annualized_return",
            "annualized_volatility",
            "sharpe_ratio",
            "max_drawdown",
            "trading_days",
            "cumulative_turnover",
            "total_transaction_cost",
            "executed_trade_count",
            "rebalance_day_count",
            "skipped_rebalance_count",
            "delist_count",
        ],
    )
    report = LiquidityCapacityReport(
        empty_summary,
        empty_buckets,
        empty_trades,
        empty_capacity,
        backtest_summary,
        pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "symbol": ["000001"],
                "target_weight": [1.0],
            }
        ),
        pd.DataFrame({"date": pd.to_datetime(["2024-01-01"]), "nav": [1.0]}),
    )

    output = write_factor_liquidity_capacity_report(
        report,
        tmp_path / "liquidity",
        parameters={"execution_day_amount_used_for_capacity": False},
    )

    assert (output / "summary.md").exists()
    assert (output / "liquidity_trades.csv").exists()
    assert (output / "liquidity_targets.csv").exists()
    payload = json.loads((output / "parameters.json").read_text(encoding="utf-8"))
    assert payload["execution_day_amount_used_for_capacity"] is False
    with pytest.raises(FileExistsError):
        write_factor_liquidity_capacity_report(
            report,
            output,
            parameters={},
        )


def test_liquidity_report_cleans_partial_directory_on_write_failure(
    tmp_path, monkeypatch
):
    report = LiquidityCapacityReport(*(pd.DataFrame() for _ in range(7)))

    def fail_to_csv(*args, **kwargs):
        raise RuntimeError("simulated report write failure")

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_to_csv)
    with pytest.raises(RuntimeError, match="simulated"):
        write_factor_liquidity_capacity_report(
            report,
            tmp_path / "liquidity",
            parameters={},
        )

    assert not (tmp_path / "liquidity").exists()
    assert list(tmp_path.glob(".liquidity.tmp-*")) == []


def test_liquidity_cli_requires_evaluation_dates_as_a_pair():
    with pytest.raises(ValueError, match="必须同时指定"):
        main_module.run_factor_liquidity_capacity_diagnostics(
            "candidate.toml",
            evaluation_start_date="2024-01-01",
        )


def test_liquidity_diagnostic_rejects_non_formal_strategy():
    class FakeDatabaseManager:
        def __init__(self):
            self.closed = False
            self.close_calls = 0
            self.opened = False

        def close_duckdb(self):
            self.closed = True
            self.close_calls += 1
            self.opened = False

    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        strategy_name="factor-composite-experiment",
        benchmark_symbol=None,
    )
    database_manager = FakeDatabaseManager()
    from backtest.liquidity_capacity_diagnostics import (
        run_factor_liquidity_capacity_diagnostic,
    )

    with pytest.raises(ValueError, match="只支持正式策略"):
        run_factor_liquidity_capacity_diagnostic(config, database_manager)
    assert database_manager.closed


def test_liquidity_diagnostic_closes_open_connection_after_load_failure(monkeypatch):
    class FakeDatabaseManager:
        def __init__(self):
            self.closed = False
            self.close_calls = 0
            self.opened = False

        def close_duckdb(self):
            self.closed = True
            self.close_calls += 1
            self.opened = False

    class FakeStrategy:
        metadata = SimpleNamespace(version="test")

        @staticmethod
        def validate_parameters(parameters):
            return {
                "factor_weights": {"test_factor": 1.0},
                "factor_versions": {"test_factor": "1"},
                "min_listing_days": 1,
            }

    def fail_after_opening_connection(self, *args, **kwargs):
        self.db_manager.opened = True
        raise RuntimeError("simulated data load failure")

    database_manager = FakeDatabaseManager()
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        strategy_name="multi-factor-quality-value-momentum",
        strategy_parameters={},
        benchmark_symbol=None,
    )
    monkeypatch.setattr(
        "backtest.liquidity_capacity_diagnostics.get_backtest_strategy",
        lambda name: FakeStrategy(),
    )
    monkeypatch.setattr(
        "backtest.liquidity_capacity_diagnostics.BacktestDataAccess.load_factor_data",
        fail_after_opening_connection,
    )

    with pytest.raises(RuntimeError, match="simulated"):
        from backtest.liquidity_capacity_diagnostics import (
            run_factor_liquidity_capacity_diagnostic,
        )

        run_factor_liquidity_capacity_diagnostic(config, database_manager)
    assert database_manager.closed
    assert database_manager.close_calls == 1
    assert not database_manager.opened
