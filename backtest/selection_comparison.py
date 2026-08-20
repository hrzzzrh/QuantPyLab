"""Research-only backtest comparison for factor selection variants."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.config import BacktestConfig
from backtest.constrained_selection_diagnostics import (
    build_constrained_targets,
)
from backtest.data_access import BacktestDataAccess
from backtest.engine import BacktestResult, DailyBacktestEngine, PreparedMarketData
from backtest.metrics import calculate_performance_metrics
from backtest.neutralization_diagnostics import build_neutralized_targets
from backtest.strategy_base import (
    TARGET_COLUMNS,
    select_equal_weight_targets,
    validate_target_weights,
)
from backtest.strategy_registry import get_backtest_strategy
from storage.database.manager import DBManager

BASELINE_VARIANT = "baseline"
NEUTRALIZATION_VARIANT_MODES = (
    "neutralized_industry",
    "neutralized_size",
    "neutralized_industry_size",
)
CONSTRAINED_VARIANT_MODES = (
    "industry_quota",
    "size_quota",
    "industry_size_quota",
)
SELECTION_VARIANTS = (
    BASELINE_VARIANT,
    *NEUTRALIZATION_VARIANT_MODES,
    *CONSTRAINED_VARIANT_MODES,
)
SELECTION_SUMMARY_COLUMNS = (
    "variant",
    "category",
    "method",
    "signal_date_count",
    "successful_signal_date_count",
    "failed_signal_date_count",
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
    "delta_total_return_vs_baseline",
    "delta_annualized_return_vs_baseline",
    "delta_max_drawdown_vs_baseline",
    "delta_turnover_vs_baseline",
    "delta_transaction_cost_vs_baseline",
)
SELECTION_COVERAGE_COLUMNS = (
    "variant",
    "date",
    "candidate_count",
    "valid_control_count",
    "control_coverage_rate",
    "selected_count",
    "holding_count",
    "holding_fill_rate",
    "status",
    "failure_reason",
)
SELECTION_OVERLAP_COLUMNS = (
    "variant",
    "date",
    "baseline_target_count",
    "variant_target_count",
    "overlap_count",
    "overlap_rate",
)
SELECTION_NAV_COLUMNS = (
    "variant",
    "date",
    "nav",
    "benchmark_nav",
    "cash",
    "positions_value",
)
SELECTION_TRADE_COLUMNS = (
    "variant",
    "date",
    "signal_date",
    "symbol",
    "side",
    "raw_open",
    "adjusted_open",
    "notional",
    "cost",
    "reason",
)
SELECTION_TARGET_COLUMNS = ("variant", *TARGET_COLUMNS)
_REQUIRED_CANDIDATE_COLUMNS = {
    "date",
    "symbol",
    "rank",
    "score",
    "industry_code",
    "market_cap",
}


@dataclass(frozen=True)
class SelectionComparisonReport:
    """Unified-engine result tables for all selection variants."""

    summary: pd.DataFrame
    daily_nav: pd.DataFrame
    trades: pd.DataFrame
    targets: pd.DataFrame
    coverage: pd.DataFrame
    target_overlap: pd.DataFrame


def build_selection_variant_targets(
    scored_candidates: pd.DataFrame,
    *,
    holding_count: int,
    quantile_count: int = 5,
) -> dict[str, pd.DataFrame]:
    """Build baseline, residualized and quota targets from one scored universe."""

    _validate_positive_integer(holding_count, "holding_count")
    _validate_positive_integer(quantile_count, "quantile_count")
    missing = _REQUIRED_CANDIDATE_COLUMNS - set(scored_candidates.columns)
    if missing:
        raise ValueError("统一选股比较候选数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = scored_candidates.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    normalized["rank"] = pd.to_numeric(normalized["rank"], errors="coerce")
    normalized["score"] = pd.to_numeric(normalized["score"], errors="coerce")
    normalized["market_cap"] = pd.to_numeric(normalized["market_cap"], errors="coerce")
    normalized["industry_code"] = (
        normalized["industry_code"].astype("string").str.strip()
    )
    if (
        normalized["date"].isna().any()
        or normalized["symbol"].isna().any()
        or normalized["symbol"].eq("").any()
        or normalized["rank"].isna().any()
        or ~np.isfinite(normalized["rank"]).all()
        or normalized["score"].isna().any()
        or ~np.isfinite(normalized["score"]).all()
    ):
        raise ValueError("统一选股比较候选数据的 date、symbol 和 score 必须有效")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("统一选股比较候选数据不能包含重复的 date/symbol")

    baseline = select_equal_weight_targets(normalized, holding_count)
    targets = {BASELINE_VARIANT: validate_target_weights(baseline)}
    for variant, mode in zip(
        NEUTRALIZATION_VARIANT_MODES,
        ("industry", "size", "industry_size"),
    ):
        targets[variant] = validate_target_weights(
            build_neutralized_targets(normalized, mode, holding_count)
        )
    for variant in CONSTRAINED_VARIANT_MODES:
        targets[variant] = validate_target_weights(
            build_constrained_targets(
                normalized,
                variant,
                holding_count,
                quantile_count,
            )
        )
    return targets


def build_selection_coverage(
    candidates: pd.DataFrame,
    variant_targets: Mapping[str, pd.DataFrame],
    *,
    holding_count: int,
) -> pd.DataFrame:
    """Build per-signal-date target coverage and failure audit rows."""

    _validate_positive_integer(holding_count, "holding_count")
    required = {"date", "symbol", "industry_code", "market_cap"}
    missing = required - set(candidates.columns)
    if missing:
        raise ValueError("选股覆盖候选数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = candidates.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    normalized["industry_code"] = (
        normalized["industry_code"].astype("string").str.strip()
    )
    normalized["market_cap"] = pd.to_numeric(normalized["market_cap"], errors="coerce")
    if (
        normalized["date"].isna().any()
        or normalized["symbol"].isna().any()
        or normalized["symbol"].eq("").any()
    ):
        raise ValueError("选股覆盖候选数据的 date 和 symbol 必须有效")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("选股覆盖候选数据不能包含重复的 date/symbol")

    candidate_counts = normalized.groupby("date", sort=True).size()
    rows = []
    for variant in SELECTION_VARIANTS:
        if variant not in variant_targets:
            raise ValueError(f"缺少选股比较目标: {variant}")
        targets = validate_target_weights(variant_targets[variant])
        _validate_target_membership(normalized, targets, variant)
        selected_counts = targets.groupby("date", sort=True).size()
        for signal_date, candidate_count in candidate_counts.items():
            candidate_group = normalized[normalized["date"] == signal_date]
            valid_control_count = int(
                _variant_control_mask(candidate_group, variant).sum()
            )
            selected_count = int(selected_counts.get(signal_date, 0))
            successful = variant == BASELINE_VARIANT or selected_count == holding_count
            if successful:
                failure_reason = None
            elif valid_control_count < holding_count:
                failure_reason = (
                    f"有效控制变量候选 {valid_control_count} 少于持仓数 {holding_count}"
                )
            else:
                failure_reason = f"目标数 {selected_count} 少于持仓数 {holding_count}"
            rows.append(
                {
                    "variant": variant,
                    "date": signal_date.date().isoformat(),
                    "candidate_count": int(candidate_count),
                    "valid_control_count": valid_control_count,
                    "control_coverage_rate": valid_control_count / candidate_count,
                    "selected_count": selected_count,
                    "holding_count": holding_count,
                    "holding_fill_rate": selected_count / holding_count,
                    "status": "success" if successful else "failed",
                    "failure_reason": failure_reason,
                }
            )
    return pd.DataFrame(rows, columns=SELECTION_COVERAGE_COLUMNS)


def build_selection_target_overlap(
    variant_targets: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    """Compare each variant's target symbols with the baseline by signal date."""

    for variant in SELECTION_VARIANTS:
        if variant not in variant_targets:
            raise ValueError(f"缺少选股比较目标: {variant}")
    normalized_targets = {
        variant: validate_target_weights(variant_targets[variant])
        for variant in SELECTION_VARIANTS
    }
    baseline_sets = _target_sets(normalized_targets[BASELINE_VARIANT])
    rows = []
    for variant in SELECTION_VARIANTS:
        target_sets = _target_sets(normalized_targets[variant])
        for signal_date, baseline_set in baseline_sets.items():
            current_set = target_sets.get(signal_date, set())
            overlap_count = len(baseline_set & current_set)
            rows.append(
                {
                    "variant": variant,
                    "date": signal_date.date().isoformat(),
                    "baseline_target_count": len(baseline_set),
                    "variant_target_count": len(current_set),
                    "overlap_count": overlap_count,
                    "overlap_rate": (
                        overlap_count / len(baseline_set) if baseline_set else None
                    ),
                }
            )
    return pd.DataFrame(rows, columns=SELECTION_OVERLAP_COLUMNS)


def compare_selection_variants(
    config: BacktestConfig,
    signal_data: pd.DataFrame,
    variant_targets: Mapping[str, pd.DataFrame],
    *,
    holding_count: int,
    candidates: pd.DataFrame | None = None,
    benchmark_prices: pd.DataFrame | None = None,
    prepared_market_data: PreparedMarketData | None = None,
) -> SelectionComparisonReport:
    """Run all target variants through one prepared daily backtest engine."""

    _validate_positive_integer(holding_count, "holding_count")
    normalized_targets = _normalize_variant_targets(variant_targets)
    if candidates is None:
        raise ValueError("统一选股比较必须提供候选股票池")
    engine = DailyBacktestEngine(config)
    prepared = prepared_market_data or engine.prepare_market_data(signal_data, config)
    coverage = build_selection_coverage(
        candidates,
        normalized_targets,
        holding_count=holding_count,
    )
    overlap = build_selection_target_overlap(normalized_targets)

    nav_frames = []
    trade_frames = []
    target_frames = []
    metric_rows = []
    raw_results: dict[str, BacktestResult] = {}
    for variant in SELECTION_VARIANTS:
        result = engine.run(
            signal_data,
            normalized_targets[variant],
            benchmark_prices,
            prepared_market_data=prepared,
        )
        raw_results[variant] = result
        nav_frame = result.daily_nav.loc[:, list(SELECTION_NAV_COLUMNS[1:])].copy()
        nav_frame.insert(0, "variant", variant)
        nav_frames.append(nav_frame)
        trade_frame = result.trades.copy()
        for column in SELECTION_TRADE_COLUMNS[1:]:
            if column not in trade_frame:
                trade_frame[column] = pd.NA
        trade_frame = trade_frame.loc[:, list(SELECTION_TRADE_COLUMNS[1:])]
        trade_frame.insert(0, "variant", variant)
        trade_frames.append(trade_frame)
        target_frame = normalized_targets[variant].copy()
        target_frame.insert(0, "variant", variant)
        target_frames.append(target_frame.loc[:, list(SELECTION_TARGET_COLUMNS)])

    baseline_metrics = calculate_performance_metrics(
        raw_results[BASELINE_VARIANT].daily_nav
    )
    baseline_trade_stats = _summarize_trades(
        raw_results[BASELINE_VARIANT].trades,
        config.initial_capital,
    )
    coverage_summary = _summarize_coverage(coverage)
    for variant in SELECTION_VARIANTS:
        result = raw_results[variant]
        metrics = calculate_performance_metrics(result.daily_nav)
        trade_stats = _summarize_trades(result.trades, config.initial_capital)
        row = {
            "variant": variant,
            "category": _variant_category(variant),
            "method": _variant_method(variant),
            **coverage_summary.get(
                variant,
                {
                    "signal_date_count": None,
                    "successful_signal_date_count": None,
                    "failed_signal_date_count": None,
                },
            ),
            **metrics,
            **trade_stats,
            "delta_total_return_vs_baseline": _delta(
                metrics["total_return"], baseline_metrics["total_return"]
            ),
            "delta_annualized_return_vs_baseline": _delta(
                metrics["annualized_return"], baseline_metrics["annualized_return"]
            ),
            "delta_max_drawdown_vs_baseline": _delta(
                metrics["max_drawdown"], baseline_metrics["max_drawdown"]
            ),
            "delta_turnover_vs_baseline": _delta(
                trade_stats["cumulative_turnover"],
                baseline_trade_stats["cumulative_turnover"],
            ),
            "delta_transaction_cost_vs_baseline": _delta(
                trade_stats["total_transaction_cost"],
                baseline_trade_stats["total_transaction_cost"],
            ),
        }
        metric_rows.append(row)

    benchmark_metrics = calculate_performance_metrics(
        raw_results[BASELINE_VARIANT].daily_nav,
        "benchmark_nav",
    )
    metric_rows.append(
        {
            "variant": "benchmark",
            "category": "benchmark",
            "method": "benchmark ETF 后复权收盘净值",
            "signal_date_count": None,
            "successful_signal_date_count": None,
            "failed_signal_date_count": None,
            **benchmark_metrics,
            "cumulative_turnover": None,
            "total_transaction_cost": None,
            "executed_trade_count": None,
            "rebalance_day_count": None,
            "skipped_rebalance_count": None,
            "delist_count": None,
            "delta_total_return_vs_baseline": _delta(
                benchmark_metrics["total_return"], baseline_metrics["total_return"]
            ),
            "delta_annualized_return_vs_baseline": _delta(
                benchmark_metrics["annualized_return"],
                baseline_metrics["annualized_return"],
            ),
            "delta_max_drawdown_vs_baseline": _delta(
                benchmark_metrics["max_drawdown"], baseline_metrics["max_drawdown"]
            ),
            "delta_turnover_vs_baseline": None,
            "delta_transaction_cost_vs_baseline": None,
        }
    )
    summary = pd.DataFrame(metric_rows, columns=SELECTION_SUMMARY_COLUMNS)
    daily_nav = pd.concat(nav_frames, ignore_index=True)
    trades = pd.concat(trade_frames, ignore_index=True)
    targets = pd.concat(target_frames, ignore_index=True)
    return SelectionComparisonReport(
        summary, daily_nav, trades, targets, coverage, overlap
    )


def run_factor_selection_variant_comparison(
    config: BacktestConfig,
    database_manager: DBManager,
    *,
    quantile_count: int = 5,
) -> SelectionComparisonReport:
    """Load one factor experiment and compare all target variants."""

    _validate_positive_integer(quantile_count, "quantile_count")
    if config.strategy_name != "factor-composite-experiment":
        raise ValueError(
            "统一选股回测比较当前只支持 factor-composite-experiment，"
            f"不支持 {config.strategy_name}"
        )
    strategy = get_backtest_strategy(config.strategy_name)
    parameters = strategy.validate_parameters(config.strategy_parameters)
    factor_names = tuple(parameters["factor_weights"])
    data_access = BacktestDataAccess(database_manager)
    signal_data = data_access.load_factor_data(
        config,
        factor_names,
        factor_parameters=parameters["factor_parameters"],
        minimum_history_days=parameters["min_listing_days"],
        include_market_cap=True,
    )
    factor_frame = strategy.calculate_factor_frame(signal_data, parameters)
    candidates = strategy.prepare_target_candidates(
        signal_data,
        factor_frame,
        config,
        parameters,
    )
    scored_candidates = strategy.score_target_candidates(candidates, parameters)
    scored_candidates = scored_candidates.merge(
        signal_data.loc[:, ["date", "symbol", "market_cap"]],
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    industry_points = data_access.load_point_in_time_industry(
        candidates.loc[:, ["date", "symbol"]]
    )
    scored_candidates = scored_candidates.merge(
        industry_points,
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    variant_targets = build_selection_variant_targets(
        scored_candidates,
        holding_count=parameters["holding_count"],
        quantile_count=quantile_count,
    )
    prepared_market_data = DailyBacktestEngine.prepare_market_data(signal_data, config)
    benchmark_prices = data_access.load_benchmark_prices(config)
    return compare_selection_variants(
        config,
        signal_data,
        variant_targets,
        holding_count=parameters["holding_count"],
        benchmark_prices=benchmark_prices,
        candidates=scored_candidates,
        prepared_market_data=prepared_market_data,
    )


def write_selection_comparison_report(
    report: SelectionComparisonReport,
    output_dir: str | Path,
    *,
    parameters: dict,
) -> Path:
    """Write unified selection comparison artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    (output_path / "parameters.json").write_text(
        json.dumps(parameters, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    report.summary.to_csv(
        output_path / "selection_comparison.csv",
        index=False,
        columns=SELECTION_SUMMARY_COLUMNS,
    )
    report.daily_nav.to_csv(
        output_path / "selection_daily_nav.csv",
        index=False,
        columns=SELECTION_NAV_COLUMNS,
    )
    report.trades.to_csv(
        output_path / "selection_trades.csv",
        index=False,
        columns=SELECTION_TRADE_COLUMNS,
    )
    report.targets.to_csv(
        output_path / "selection_targets.csv",
        index=False,
        columns=SELECTION_TARGET_COLUMNS,
    )
    report.coverage.to_csv(
        output_path / "selection_coverage.csv",
        index=False,
        columns=SELECTION_COVERAGE_COLUMNS,
    )
    report.target_overlap.to_csv(
        output_path / "selection_target_overlap.csv",
        index=False,
        columns=SELECTION_OVERLAP_COLUMNS,
    )
    (output_path / "summary.md").write_text(
        _build_summary(report, parameters),
        encoding="utf-8",
    )
    return output_path


def _normalize_variant_targets(
    variant_targets: Mapping[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    if not isinstance(variant_targets, Mapping):
        raise ValueError("选股比较目标必须是映射")
    missing = set(SELECTION_VARIANTS) - set(variant_targets)
    extra = set(variant_targets) - set(SELECTION_VARIANTS)
    if missing or extra:
        details = []
        if missing:
            details.append("缺少 " + ", ".join(sorted(missing)))
        if extra:
            details.append("不支持 " + ", ".join(sorted(extra)))
        raise ValueError("选股比较目标集合不完整: " + "；".join(details))
    return {
        variant: validate_target_weights(variant_targets[variant])
        for variant in SELECTION_VARIANTS
    }


def _target_sets(targets: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
    return {
        signal_date: set(group["symbol"])
        for signal_date, group in targets.groupby("date", sort=True)
    }


def _validate_target_membership(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
    variant: str,
) -> None:
    if targets.empty:
        return
    candidate_keys = candidates.loc[:, ["date", "symbol"]].copy()
    candidate_keys["date"] = pd.to_datetime(candidate_keys["date"], errors="coerce")
    candidate_keys["symbol"] = candidate_keys["symbol"].astype("string").str.strip()
    target_keys = targets.loc[:, ["date", "symbol"]].copy()
    target_keys["date"] = pd.to_datetime(target_keys["date"], errors="coerce")
    target_keys["symbol"] = target_keys["symbol"].astype("string").str.strip()
    membership = target_keys.merge(
        candidate_keys,
        on=["date", "symbol"],
        how="left",
        indicator=True,
    )
    if (membership["_merge"] != "both").any():
        raise ValueError(f"选股比较 {variant} 目标必须属于对应信号日候选池")


def _variant_control_mask(group: pd.DataFrame, variant: str) -> pd.Series:
    valid_industry = group["industry_code"].notna() & group["industry_code"].ne("")
    valid_size = (
        group["market_cap"].notna()
        & np.isfinite(group["market_cap"])
        & group["market_cap"].gt(0)
    )
    if variant == BASELINE_VARIANT:
        return pd.Series(True, index=group.index)
    if variant in {"neutralized_industry", "industry_quota"}:
        return valid_industry
    if variant in {"neutralized_size", "size_quota"}:
        return valid_size
    return valid_industry & valid_size


def _summarize_coverage(coverage: pd.DataFrame) -> dict[str, dict[str, int | None]]:
    if coverage.empty:
        return {}
    summary = {}
    for variant, group in coverage.groupby("variant", sort=False):
        summary[variant] = {
            "signal_date_count": int(len(group)),
            "successful_signal_date_count": int((group["status"] == "success").sum()),
            "failed_signal_date_count": int((group["status"] == "failed").sum()),
        }
    return summary


def _summarize_trades(trades: pd.DataFrame, initial_capital: float) -> dict:
    if initial_capital <= 0:
        raise ValueError("初始资金必须为正数")
    if trades.empty:
        return {
            "cumulative_turnover": 0.0,
            "total_transaction_cost": 0.0,
            "executed_trade_count": 0,
            "rebalance_day_count": 0,
            "skipped_rebalance_count": 0,
            "delist_count": 0,
        }
    side = trades["side"].astype("string")
    execution_mask = side.isin(["BUY", "SELL"])
    notional = pd.to_numeric(trades["notional"], errors="coerce").fillna(0.0)
    costs = pd.to_numeric(trades["cost"], errors="coerce").fillna(0.0)
    reason = trades["reason"].astype("string")
    return {
        "cumulative_turnover": float(notional[execution_mask].sum() / initial_capital),
        "total_transaction_cost": float(costs.sum()),
        "executed_trade_count": int(execution_mask.sum()),
        "rebalance_day_count": int(
            trades.loc[reason == "monthly_rebalance", "date"].nunique()
        ),
        "skipped_rebalance_count": int((side == "SKIP_REBALANCE").sum()),
        "delist_count": int((side == "DELIST").sum()),
    }


def _variant_category(variant: str) -> str:
    if variant == BASELINE_VARIANT:
        return "baseline"
    if variant in NEUTRALIZATION_VARIANT_MODES:
        return "neutralization"
    return "quota"


def _variant_method(variant: str) -> str:
    methods = {
        BASELINE_VARIANT: "原始综合 score 降序等权",
        "neutralized_industry": "行业哑变量残差降序等权",
        "neutralized_size": "log(market_cap) 残差降序等权",
        "neutralized_industry_size": "行业哑变量 + log(market_cap) 残差降序等权",
        "industry_quota": "Hamilton 行业比例配额，组内原始 score",
        "size_quota": "Hamilton 规模比例配额，组内原始 score",
        "industry_size_quota": "Hamilton 行业×规模比例配额，组内原始 score",
    }
    return methods.get(variant, "—")


def _delta(value, baseline):
    if value is None or baseline is None or pd.isna(value) or pd.isna(baseline):
        return None
    return float(value - baseline)


def _build_summary(
    report: SelectionComparisonReport,
    parameters: dict,
) -> str:
    lines = [
        "# 因子选股变体统一成本与成交回测比较",
        "",
        "本报告把基准、残差化和比例配额目标送入同一 T+1 开盘、后复权、手续费与滑点引擎；它是研究比较，不改变正式策略默认值。",
        "",
        "## 1. 评估信息",
        "",
    ]
    _append_table(
        lines,
        ["项目", "内容"],
        [
            ("生成时间", datetime.now().astimezone().isoformat(timespec="seconds")),
            ("原始配置", parameters.get("backtest_config_path", "—")),
            ("实际评估区间", parameters.get("evaluation_scope", "—")),
            ("基准行情", parameters.get("benchmark_data_status", "—")),
            ("成交规则", "信号日收盘生成，下一实际交易日开盘执行"),
            (
                "成本规则",
                f"commission_bps={parameters.get('commission_bps', '—')}, slippage_bps={parameters.get('slippage_bps', '—')}",
            ),
            ("变体", ", ".join(SELECTION_VARIANTS)),
        ],
    )
    lines.extend(["", "## 2. 指标比较", ""])
    benchmark_row = report.summary.loc[report.summary["variant"] == "benchmark"]
    if benchmark_row.empty or benchmark_row.iloc[0]["trading_days"] == 0:
        lines.extend(
            [
                "> ⚠️ 当前评估区间没有可用的基准 ETF 行情，基准收益列为空；策略变体之间的比较仍使用同一成本和成交引擎，但不能据此解读相对基准超额收益。",
                "",
            ]
        )
    _append_table(
        lines,
        [
            "变体",
            "成功信号日",
            "失败信号日",
            "总收益",
            "年化收益",
            "年化波动",
            "夏普",
            "最大回撤",
            "累计换手",
            "交易成本",
            "相对 baseline 总收益",
        ],
        [
            (
                row["variant"],
                _format_integer(row["successful_signal_date_count"]),
                _format_integer(row["failed_signal_date_count"]),
                _format_percent(row["total_return"]),
                _format_percent(row["annualized_return"]),
                _format_percent(row["annualized_volatility"]),
                _format_number(row["sharpe_ratio"]),
                _format_percent(row["max_drawdown"]),
                _format_number(row["cumulative_turnover"]),
                _format_number(row["total_transaction_cost"]),
                _format_percent(row["delta_total_return_vs_baseline"]),
            )
            for _, row in report.summary.iterrows()
        ],
    )
    lines.extend(
        [
            "",
            "## 3. 解释边界",
            "",
            "只有在调用方预先锁定、且未参与方案选择的日期区间上，结果才可作为样本外比较。缺失控制变量或有效目标不足的信号日不会回退；收益、低回撤、低换手或低成本均不能单独证明选股约束具有因果改善。",
            "",
            "换手只统计 BUY/SELL 的单边名义金额除以初始资金；DELIST 和 SKIP_REBALANCE 单独计数，交易成本汇总成交记录 cost。",
            "",
            "## 4. 审计文件",
            "",
        ]
    )
    _append_table(
        lines,
        ["文件", "内容"],
        [
            ("parameters.json", "原始配置、实际评估配置、数据源和口径"),
            (
                "selection_comparison.csv",
                "各变体收益、风险、换手、成本和相对 baseline 差异",
            ),
            ("selection_daily_nav.csv", "逐变体逐交易日净值和基准净值"),
            ("selection_trades.csv", "逐变体逐笔成交、清算、跳过和成本"),
            ("selection_targets.csv", "逐变体信号日目标权重"),
            ("selection_coverage.csv", "逐变体逐信号日目标覆盖和失败原因"),
            ("selection_target_overlap.csv", "逐变体与 baseline 的目标重合"),
        ],
    )
    return "\n".join(lines) + "\n"


def _append_table(lines: list[str], headers, rows) -> None:
    lines.append("| " + " | ".join(str(header) for header in headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(_format_value(value) for value in row) + " |")


def _format_value(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return str(value).replace("|", "\\|").replace("\n", " ")


def _format_percent(value) -> str:
    return "—" if value is None or pd.isna(value) else f"{float(value):.2%}"


def _format_number(value) -> str:
    return "—" if value is None or pd.isna(value) else f"{float(value):.2f}"


def _format_integer(value) -> str:
    return "—" if value is None or pd.isna(value) else str(int(value))


def _validate_positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} 必须是正整数")
