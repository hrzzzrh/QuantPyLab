"""Research-only marginal contribution diagnostics for multi-factor portfolios."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from analysis.factors import FactorEngine
from analysis.factors.transforms import filter_valid_factor_rows
from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.engine import DailyBacktestEngine, PreparedMarketData
from backtest.metrics import calculate_performance_metrics
from backtest.selection_comparison import (
    SELECTION_NAV_COLUMNS,
    SELECTION_TARGET_COLUMNS,
    SELECTION_TRADE_COLUMNS,
    run_target_backtests,
    summarize_trade_statistics,
)
from backtest.strategies.factor_composite_experiment import (
    FactorCompositeExperimentStrategy,
)
from backtest.strategy_base import (
    TARGET_COLUMNS,
    select_equal_weight_targets,
    validate_target_weights,
)
from backtest.strategy_registry import get_backtest_strategy
from backtest.trading_calendar import get_confirmed_month_end_trading_dates
from storage.database.manager import DBManager

FULL_COMBINATION_VARIANT = "full_combination"
MARGINAL_SUMMARY_COLUMNS = (
    "variant",
    "variant_type",
    "factor_name",
    "factor_count",
    "factor_weights",
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
    "mean_target_overlap_rate",
    "min_target_overlap_rate",
    "delta_total_return_vs_full",
    "delta_annualized_return_vs_full",
    "delta_max_drawdown_vs_full",
    "delta_turnover_vs_full",
    "delta_transaction_cost_vs_full",
)
MARGINAL_COVERAGE_COLUMNS = (
    "variant",
    "date",
    "candidate_count",
    "selected_count",
    "holding_count",
    "holding_fill_rate",
    "status",
    "failure_reason",
)
MARGINAL_OVERLAP_COLUMNS = (
    "variant",
    "date",
    "full_target_count",
    "variant_target_count",
    "overlap_count",
    "overlap_rate",
)
MARGINAL_TARGET_COLUMNS = ("variant", *TARGET_COLUMNS)


@dataclass(frozen=True)
class MarginalVariantSpec:
    """One factor subset and its normalized weight protocol."""

    variant: str
    variant_type: str
    factor_name: str | None
    factor_weights: dict[str, float]


@dataclass(frozen=True)
class MarginalContributionReport:
    """Audit tables for multi-factor marginal contribution backtests."""

    summary: pd.DataFrame
    daily_nav: pd.DataFrame
    trades: pd.DataFrame
    targets: pd.DataFrame
    coverage: pd.DataFrame
    target_overlap: pd.DataFrame


def build_marginal_variant_specs(
    formal_factor_weights: Mapping[str, float],
) -> tuple[MarginalVariantSpec, ...]:
    """Build full, single-factor and leave-one-out weight variants."""

    if not isinstance(formal_factor_weights, Mapping) or not formal_factor_weights:
        raise ValueError("正式因子权重必须是非空映射")
    validated: dict[str, float] = {}
    for factor_name, weight in formal_factor_weights.items():
        if not isinstance(factor_name, str) or not factor_name:
            raise ValueError("正式因子名称必须是非空字符串")
        if (
            isinstance(weight, bool)
            or not isinstance(weight, (int, float))
            or not math.isfinite(weight)
            or weight <= 0
        ):
            raise ValueError(f"因子 {factor_name} 的权重必须是正的有限数字")
        validated[factor_name] = float(weight)
    total_weight = sum(validated.values())
    if total_weight <= 0:
        raise ValueError("正式因子权重不能全部为零")
    normalized = {name: weight / total_weight for name, weight in validated.items()}
    if len(normalized) < 2:
        raise ValueError("边际贡献验证至少需要两个正权重因子")

    specs = [
        MarginalVariantSpec(
            FULL_COMBINATION_VARIANT,
            "full_combination",
            None,
            normalized,
        )
    ]
    for factor_name in normalized:
        specs.append(
            MarginalVariantSpec(
                f"single_{factor_name}",
                "single_factor",
                factor_name,
                {factor_name: 1.0},
            )
        )
    for factor_name in normalized:
        remaining_total = 1.0 - normalized[factor_name]
        remaining = {
            name: weight / remaining_total
            for name, weight in normalized.items()
            if name != factor_name
        }
        specs.append(
            MarginalVariantSpec(
                f"leave_one_out_{factor_name}",
                "leave_one_out",
                factor_name,
                remaining,
            )
        )
    return tuple(specs)


def build_common_factor_candidates(
    signal_data: pd.DataFrame,
    factor_names: Sequence[str],
    config: BacktestConfig,
    *,
    minimum_history_days: int,
) -> pd.DataFrame:
    """Build one universe where every formal factor is point-in-time valid."""

    if not factor_names:
        raise ValueError("共同候选池至少需要一个因子")
    if (
        isinstance(minimum_history_days, bool)
        or not isinstance(minimum_history_days, int)
        or minimum_history_days < 0
    ):
        raise ValueError("minimum_history_days 必须是非负整数")
    factor_frame = FactorEngine().calculate(signal_data, tuple(factor_names))
    ordered_input = signal_data.copy()
    ordered_input["date"] = pd.to_datetime(ordered_input["date"])
    ordered_input = ordered_input.sort_values(["symbol", "date"])
    listing_days = ordered_input[["date", "symbol"]].copy()
    listing_days["listing_days"] = (
        ordered_input.groupby("symbol", sort=False).cumcount() + 1
    ).to_numpy()
    candidates = factor_frame.merge(
        listing_days,
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    candidates = candidates[
        candidates["date"].isin(
            get_confirmed_month_end_trading_dates(candidates["date"])
        )
    ].copy()
    candidates = candidates[candidates["date"].dt.date >= config.start_date]
    candidates = candidates[candidates["listing_days"] >= minimum_history_days].copy()
    candidates = filter_valid_factor_rows(candidates, tuple(factor_names))
    if candidates.empty:
        raise ValueError("七因子共同候选池为空")
    return candidates


def build_marginal_targets(
    candidates: pd.DataFrame,
    specs: Sequence[MarginalVariantSpec],
    *,
    holding_count: int,
    winsorize_lower: float,
    winsorize_upper: float,
) -> dict[str, pd.DataFrame]:
    """Score each weight variant on the same common candidate universe."""

    if isinstance(holding_count, bool) or not isinstance(holding_count, int):
        raise ValueError("holding_count 必须是正整数")
    if holding_count <= 0:
        raise ValueError("holding_count 必须是正整数")
    if not 0 <= winsorize_lower < winsorize_upper <= 1:
        raise ValueError("缩尾范围必须满足 0 <= lower < upper <= 1")
    if not specs:
        raise ValueError("边际贡献变体不能为空")
    required = {"date", "symbol"}
    factor_names = set()
    for spec in specs:
        factor_names.update(spec.factor_weights)
    required.update(factor_names)
    missing = required - set(candidates.columns)
    if missing:
        raise ValueError("边际贡献候选数据缺少字段: " + ", ".join(sorted(missing)))
    if candidates.duplicated(["date", "symbol"]).any():
        raise ValueError("边际贡献候选数据不能包含重复的 date/symbol")

    targets = {}
    for spec in specs:
        parameters = {
            "factor_weights": spec.factor_weights,
            "winsorize_lower": winsorize_lower,
            "winsorize_upper": winsorize_upper,
        }
        scored = FactorCompositeExperimentStrategy.score_target_candidates(
            candidates,
            parameters,
        )
        targets[spec.variant] = validate_target_weights(
            select_equal_weight_targets(scored, holding_count)
        )
    return targets


def build_marginal_coverage(
    candidates: pd.DataFrame,
    targets: Mapping[str, pd.DataFrame],
    *,
    holding_count: int,
    variant_order: Sequence[str],
) -> pd.DataFrame:
    """Build common-universe and target-fill coverage rows."""

    candidate_counts = candidates.groupby("date", sort=True).size()
    rows = []
    for variant in variant_order:
        if variant not in targets:
            raise ValueError(f"缺少边际贡献目标: {variant}")
        normalized_targets = validate_target_weights(targets[variant])
        _validate_target_membership(candidates, normalized_targets, variant)
        selected_counts = normalized_targets.groupby("date", sort=True).size()
        for signal_date, candidate_count in candidate_counts.items():
            selected_count = int(selected_counts.get(signal_date, 0))
            success = selected_count == holding_count
            rows.append(
                {
                    "variant": variant,
                    "date": signal_date.date().isoformat(),
                    "candidate_count": int(candidate_count),
                    "selected_count": selected_count,
                    "holding_count": holding_count,
                    "holding_fill_rate": selected_count / holding_count,
                    "status": "success" if success else "failed",
                    "failure_reason": (
                        None
                        if success
                        else f"目标数 {selected_count} 少于持仓数 {holding_count}"
                    ),
                }
            )
    return pd.DataFrame(rows, columns=MARGINAL_COVERAGE_COLUMNS)


def build_marginal_target_overlap(
    targets: Mapping[str, pd.DataFrame],
    *,
    variant_order: Sequence[str],
) -> pd.DataFrame:
    """Compare each target set with the full combination."""

    normalized = {
        variant: validate_target_weights(targets[variant]) for variant in variant_order
    }
    full_sets = _target_sets(normalized[FULL_COMBINATION_VARIANT])
    rows = []
    for variant in variant_order:
        current_sets = _target_sets(normalized[variant])
        for signal_date, full_set in full_sets.items():
            current_set = current_sets.get(signal_date, set())
            overlap_count = len(full_set & current_set)
            rows.append(
                {
                    "variant": variant,
                    "date": signal_date.date().isoformat(),
                    "full_target_count": len(full_set),
                    "variant_target_count": len(current_set),
                    "overlap_count": overlap_count,
                    "overlap_rate": (
                        overlap_count / len(full_set) if full_set else None
                    ),
                }
            )
    return pd.DataFrame(rows, columns=MARGINAL_OVERLAP_COLUMNS)


def compare_marginal_contributions(
    config: BacktestConfig,
    signal_data: pd.DataFrame,
    candidates: pd.DataFrame,
    specs: Sequence[MarginalVariantSpec],
    targets: Mapping[str, pd.DataFrame],
    *,
    holding_count: int,
    benchmark_prices: pd.DataFrame | None = None,
    prepared_market_data: PreparedMarketData | None = None,
    confirmed_delisting_dates: Mapping[str, object] | None = None,
) -> MarginalContributionReport:
    """Run all marginal variants through one prepared daily engine."""

    variant_order = tuple(spec.variant for spec in specs)
    if not variant_order or variant_order[0] != FULL_COMBINATION_VARIANT:
        raise ValueError("边际贡献变体必须以 full_combination 开始")
    normalized_targets = {
        variant: validate_target_weights(targets[variant]) for variant in variant_order
    }
    coverage = build_marginal_coverage(
        candidates,
        normalized_targets,
        holding_count=holding_count,
        variant_order=variant_order,
    )
    overlap = build_marginal_target_overlap(
        normalized_targets,
        variant_order=variant_order,
    )
    raw_results = run_target_backtests(
        config,
        signal_data,
        normalized_targets,
        benchmark_prices=benchmark_prices,
        prepared_market_data=prepared_market_data,
        confirmed_delisting_dates=confirmed_delisting_dates,
    )
    full_metrics = calculate_performance_metrics(
        raw_results[FULL_COMBINATION_VARIANT].daily_nav
    )
    full_trade_stats = summarize_trade_statistics(
        raw_results[FULL_COMBINATION_VARIANT].trades,
        config.initial_capital,
    )
    coverage_summary = _summarize_coverage(coverage)
    overlap_summary = _summarize_overlap(overlap)
    summary_rows = []
    nav_frames = []
    trade_frames = []
    target_frames = []
    spec_map = {spec.variant: spec for spec in specs}
    for variant in variant_order:
        result = raw_results[variant]
        spec = spec_map[variant]
        metrics = calculate_performance_metrics(result.daily_nav)
        trade_stats = summarize_trade_statistics(
            result.trades,
            config.initial_capital,
        )
        summary_rows.append(
            {
                "variant": variant,
                "variant_type": spec.variant_type,
                "factor_name": spec.factor_name,
                "factor_count": len(spec.factor_weights),
                "factor_weights": json.dumps(
                    spec.factor_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                **coverage_summary[variant],
                **metrics,
                **trade_stats,
                **overlap_summary[variant],
                "delta_total_return_vs_full": _delta(
                    metrics["total_return"], full_metrics["total_return"]
                ),
                "delta_annualized_return_vs_full": _delta(
                    metrics["annualized_return"], full_metrics["annualized_return"]
                ),
                "delta_max_drawdown_vs_full": _delta(
                    metrics["max_drawdown"], full_metrics["max_drawdown"]
                ),
                "delta_turnover_vs_full": _delta(
                    trade_stats["cumulative_turnover"],
                    full_trade_stats["cumulative_turnover"],
                ),
                "delta_transaction_cost_vs_full": _delta(
                    trade_stats["total_transaction_cost"],
                    full_trade_stats["total_transaction_cost"],
                ),
            }
        )
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

    benchmark_metrics = calculate_performance_metrics(
        raw_results[FULL_COMBINATION_VARIANT].daily_nav,
        "benchmark_nav",
    )
    summary_rows.append(
        {
            "variant": "benchmark",
            "variant_type": "benchmark",
            "factor_name": None,
            "factor_count": None,
            "factor_weights": None,
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
            "mean_target_overlap_rate": None,
            "min_target_overlap_rate": None,
            "delta_total_return_vs_full": _delta(
                benchmark_metrics["total_return"], full_metrics["total_return"]
            ),
            "delta_annualized_return_vs_full": _delta(
                benchmark_metrics["annualized_return"],
                full_metrics["annualized_return"],
            ),
            "delta_max_drawdown_vs_full": _delta(
                benchmark_metrics["max_drawdown"], full_metrics["max_drawdown"]
            ),
            "delta_turnover_vs_full": None,
            "delta_transaction_cost_vs_full": None,
        }
    )
    return MarginalContributionReport(
        pd.DataFrame(summary_rows, columns=MARGINAL_SUMMARY_COLUMNS),
        pd.concat(nav_frames, ignore_index=True),
        pd.concat(trade_frames, ignore_index=True),
        pd.concat(target_frames, ignore_index=True),
        coverage,
        overlap,
    )


def run_factor_marginal_contribution(
    config: BacktestConfig,
    database_manager: DBManager,
) -> tuple[MarginalContributionReport, dict]:
    """Load the formal multi-factor strategy and run marginal contribution variants."""

    if config.strategy_name != "multi-factor-quality-value-momentum":
        raise ValueError(
            "多因子边际贡献验证只支持 multi-factor-quality-value-momentum，"
            f"不支持 {config.strategy_name}"
        )
    strategy = get_backtest_strategy(config.strategy_name)
    parameters = strategy.validate_parameters(config.strategy_parameters)
    factor_names = tuple(parameters["factor_weights"])
    data_access = BacktestDataAccess(database_manager)
    signal_data = data_access.load_factor_data(
        config,
        factor_names,
        minimum_history_days=parameters["min_listing_days"],
    )
    candidates = build_common_factor_candidates(
        signal_data,
        factor_names,
        config,
        minimum_history_days=parameters["min_listing_days"],
    )
    specs = build_marginal_variant_specs(parameters["factor_weights"])
    targets = build_marginal_targets(
        candidates,
        specs,
        holding_count=parameters["holding_count"],
        winsorize_lower=parameters["winsorize_lower"],
        winsorize_upper=parameters["winsorize_upper"],
    )
    prepared_market_data = DailyBacktestEngine.prepare_market_data(signal_data, config)
    confirmed_delisting_dates = data_access.load_confirmed_delisting_dates(
        signal_data["symbol"].drop_duplicates().tolist(),
        config.end_date,
    )
    benchmark_prices = data_access.load_benchmark_prices(config)
    report = compare_marginal_contributions(
        config,
        signal_data,
        candidates,
        specs,
        targets,
        holding_count=parameters["holding_count"],
        benchmark_prices=benchmark_prices,
        prepared_market_data=prepared_market_data,
        confirmed_delisting_dates=confirmed_delisting_dates,
    )
    metadata = {
        "factor_names": list(factor_names),
        "factor_versions": parameters["factor_versions"],
        "formal_factor_weights": parameters["factor_weights"],
        "variant_specs": [
            {
                "variant": spec.variant,
                "variant_type": spec.variant_type,
                "factor_name": spec.factor_name,
                "factor_weights": spec.factor_weights,
            }
            for spec in specs
        ],
        "holding_count": parameters["holding_count"],
        "min_listing_days": parameters["min_listing_days"],
        "winsorize_lower": parameters["winsorize_lower"],
        "winsorize_upper": parameters["winsorize_upper"],
        "common_candidate_rows": int(len(candidates)),
        "common_candidate_signal_dates": int(candidates["date"].nunique()),
        "industry_data_status": "not_used",
    }
    return report, metadata


def write_marginal_contribution_report(
    report: MarginalContributionReport,
    output_dir: str | Path,
    *,
    parameters: dict,
) -> Path:
    """Write marginal contribution comparison artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    (output_path / "parameters.json").write_text(
        json.dumps(parameters, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    report.summary.to_csv(
        output_path / "marginal_contribution.csv",
        index=False,
        columns=MARGINAL_SUMMARY_COLUMNS,
    )
    report.daily_nav.to_csv(
        output_path / "marginal_daily_nav.csv",
        index=False,
        columns=SELECTION_NAV_COLUMNS,
    )
    report.trades.to_csv(
        output_path / "marginal_trades.csv",
        index=False,
        columns=SELECTION_TRADE_COLUMNS,
    )
    report.targets.to_csv(
        output_path / "marginal_targets.csv",
        index=False,
        columns=MARGINAL_TARGET_COLUMNS,
    )
    report.coverage.to_csv(
        output_path / "marginal_coverage.csv",
        index=False,
        columns=MARGINAL_COVERAGE_COLUMNS,
    )
    report.target_overlap.to_csv(
        output_path / "marginal_target_overlap.csv",
        index=False,
        columns=MARGINAL_OVERLAP_COLUMNS,
    )
    (output_path / "summary.md").write_text(
        _build_summary(report, parameters),
        encoding="utf-8",
    )
    return output_path


def _validate_target_membership(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
    variant: str,
) -> None:
    if targets.empty:
        return
    candidate_keys = candidates.loc[:, ["date", "symbol"]].copy()
    target_keys = targets.loc[:, ["date", "symbol"]].copy()
    for frame in (candidate_keys, target_keys):
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["symbol"] = frame["symbol"].astype("string").str.strip()
    membership = target_keys.merge(
        candidate_keys,
        on=["date", "symbol"],
        how="left",
        indicator=True,
    )
    if (membership["_merge"] != "both").any():
        raise ValueError(f"边际贡献 {variant} 目标必须属于共同候选池")


def _target_sets(targets: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
    return {
        signal_date: set(group["symbol"])
        for signal_date, group in targets.groupby("date", sort=True)
    }


def _summarize_coverage(coverage: pd.DataFrame) -> dict[str, dict[str, int | None]]:
    return {
        variant: {
            "signal_date_count": int(len(group)),
            "successful_signal_date_count": int((group["status"] == "success").sum()),
            "failed_signal_date_count": int((group["status"] == "failed").sum()),
        }
        for variant, group in coverage.groupby("variant", sort=False)
    }


def _summarize_overlap(overlap: pd.DataFrame) -> dict[str, dict[str, float | None]]:
    return {
        variant: {
            "mean_target_overlap_rate": group["overlap_rate"].mean(),
            "min_target_overlap_rate": group["overlap_rate"].min(),
        }
        for variant, group in overlap.groupby("variant", sort=False)
    }


def _delta(value, baseline):
    if value is None or baseline is None or pd.isna(value) or pd.isna(baseline):
        return None
    return float(value - baseline)


def _build_summary(report: MarginalContributionReport, parameters: dict) -> str:
    lines = [
        "# 多因子组合边际贡献验证",
        "",
        "本报告在七因子共同有效候选池上比较完整组合、单因子和 leave-one-out 组合；全部目标使用同一 T+1 开盘、后复权、手续费与滑点引擎。",
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
            (
                "共同候选池",
                f"{parameters.get('common_candidate_rows', '—')} 行，{parameters.get('common_candidate_signal_dates', '—')} 个信号日",
            ),
            ("权重协议", "正式组合；单因子；删除一个因子后剩余正式权重重新归一化"),
            ("基准行情", parameters.get("benchmark_data_status", "—")),
        ],
    )
    lines.extend(["", "## 2. 组合比较", ""])
    benchmark_row = report.summary.loc[report.summary["variant"] == "benchmark"]
    if benchmark_row.empty or benchmark_row.iloc[0]["trading_days"] == 0:
        lines.extend(
            [
                "> ⚠️ 当前评估区间没有可用的基准 ETF 行情，基准收益列为空；只能比较因子变体之间的结果。",
                "",
            ]
        )
    _append_table(
        lines,
        [
            "变体",
            "类型",
            "成功/失败信号日",
            "总收益",
            "年化收益",
            "年化波动",
            "夏普",
            "最大回撤",
            "累计换手",
            "成本",
            "与完整组合重合",
            "相对完整组合收益",
        ],
        [
            (
                row["variant"],
                row["variant_type"],
                f"{_format_integer(row['successful_signal_date_count'])}/{_format_integer(row['failed_signal_date_count'])}",
                _format_percent(row["total_return"]),
                _format_percent(row["annualized_return"]),
                _format_percent(row["annualized_volatility"]),
                _format_number(row["sharpe_ratio"]),
                _format_percent(row["max_drawdown"]),
                _format_number(row["cumulative_turnover"]),
                _format_number(row["total_transaction_cost"]),
                _format_percent(row["mean_target_overlap_rate"]),
                _format_percent(row["delta_total_return_vs_full"]),
            )
            for _, row in report.summary.iterrows()
        ],
    )
    lines.extend(
        [
            "",
            "## 3. 解释边界",
            "",
            "共同候选池只消除了因子缺失造成的股票池差异，不能消除因子相关性、测试区间长度和多重比较风险。单因子收益高不等于应提高正式权重；leave-one-out 差异同时包含重新归一化、排名和交易路径变化。该报告不自动修改正式策略。",
            "",
            "## 4. 审计文件",
            "",
        ]
    )
    _append_table(
        lines,
        ["文件", "内容"],
        [
            ("parameters.json", "配置、因子版本、权重协议、共同候选池和基准状态"),
            (
                "marginal_contribution.csv",
                "15 个变体的收益、风险、成本和相对完整组合差异",
            ),
            ("marginal_daily_nav.csv", "逐变体逐交易日净值和基准净值"),
            ("marginal_trades.csv", "逐变体逐笔成交、清算、跳过和成本"),
            ("marginal_targets.csv", "逐变体信号日目标权重"),
            ("marginal_coverage.csv", "共同候选池和目标填充覆盖"),
            ("marginal_target_overlap.csv", "逐变体与完整组合目标重合"),
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
