"""Point-in-time size exposure diagnostics for factor experiment targets."""

import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

SIZE_EXPOSURE_COLUMNS = (
    "date",
    "size_bucket",
    "quantile_count",
    "universe_count",
    "selected_count",
    "universe_share",
    "selected_share",
    "selection_lift",
    "universe_market_cap_median",
    "selected_market_cap_median",
    "valid_market_cap_count",
    "selected_valid_market_cap_count",
    "market_cap_coverage_rate",
    "selected_market_cap_coverage_rate",
)
SIZE_EXPOSURE_SUMMARY_COLUMNS = (
    "size_bucket",
    "quantile_count",
    "signal_date_count",
    "mean_universe_share",
    "mean_selected_share",
    "mean_selection_lift",
    "mean_share_difference",
    "median_universe_market_cap",
    "median_selected_market_cap",
    "mean_market_cap_coverage_rate",
    "mean_selected_market_cap_coverage_rate",
)
SIZE_EXPOSURE_COVERAGE_COLUMNS = (
    "date",
    "candidate_count",
    "valid_market_cap_count",
    "market_cap_coverage_rate",
    "selected_count",
    "selected_valid_market_cap_count",
    "selected_market_cap_coverage_rate",
)
SIZE_EXPOSURE_LIFT_WARNING_THRESHOLD = 1.25
SIZE_EXPOSURE_AVOIDANCE_WARNING_THRESHOLD = 0.75


@dataclass(frozen=True)
class SizeExposureDiagnosticReport:
    """The per-signal and aggregate size exposure tables."""

    exposure: pd.DataFrame
    summary: pd.DataFrame
    coverage: pd.DataFrame


def calculate_size_exposure_diagnostics(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    quantile_count: int = 5,
) -> SizeExposureDiagnosticReport:
    """Compare selected holdings with the point-in-time size universe.

    Size buckets are calculated independently for every signal date. Candidates
    without a positive finite market cap remain in the coverage audit but are
    excluded from the bucket shares. Selected shares use selected holdings with
    valid market caps as their denominator.
    """

    quantile_count = _validate_quantile_count(quantile_count)
    normalized_candidates = _normalize_candidates(candidates)
    normalized_targets = _normalize_targets(targets)
    if normalized_candidates.empty:
        raise ValueError("规模暴露诊断没有可选股票池")
    target_membership = normalized_targets.merge(
        normalized_candidates.loc[:, ["date", "symbol"]],
        on=["date", "symbol"],
        how="left",
        indicator=True,
    )
    if (target_membership["_merge"] != "both").any():
        raise ValueError("规模暴露诊断目标必须属于对应信号日的可选股票池")

    candidate_counts = normalized_candidates.groupby("date", sort=True).size()
    selected_counts = normalized_targets.groupby("date", sort=True).size()
    merged = normalized_candidates.merge(
        normalized_targets.loc[:, ["date", "symbol"]].assign(selected=True),
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    merged["selected"] = merged["selected"].fillna(False).astype(bool)
    merged["market_cap"] = pd.to_numeric(merged["market_cap"], errors="coerce")
    valid_market_cap = (
        merged["market_cap"].notna()
        & merged["market_cap"].map(math.isfinite)
        & merged["market_cap"].gt(0)
    )
    valid = merged.loc[valid_market_cap].copy()
    if not valid.empty:
        valid = valid.sort_values(["date", "symbol"], kind="mergesort")
    valid_counts = valid.groupby("date", sort=True).size()
    insufficient_dates = [
        signal_date
        for signal_date in candidate_counts.index
        if int(valid_counts.get(signal_date, 0)) < quantile_count
    ]
    if insufficient_dates:
        raise ValueError(
            f"每个信号日至少需要 {quantile_count} 个有效市值候选，"
            f"但 {len(insufficient_dates)} 个信号日不满足"
        )
    if not valid.empty:
        valid["size_rank"] = valid.groupby("date", sort=False)["market_cap"].rank(
            method="first", pct=True, ascending=True
        )
        valid["size_bucket"] = (
            (valid["size_rank"] * quantile_count)
            .apply(math.ceil)
            .clip(lower=1, upper=quantile_count)
            .astype(int)
        )

    selected_valid_counts = (
        valid.loc[valid["selected"]].groupby("date", sort=True).size()
    )
    exposure_rows = []
    for (signal_date, size_bucket), group in valid.groupby(
        ["date", "size_bucket"], sort=True
    ):
        valid_count = int(valid_counts.loc[signal_date])
        selected_valid_count = int(selected_valid_counts.get(signal_date, 0))
        universe_count = len(group)
        selected_count = int(group["selected"].sum())
        universe_share = universe_count / valid_count if valid_count else None
        selected_share = (
            selected_count / selected_valid_count if selected_valid_count else None
        )
        selection_lift = (
            selected_share / universe_share
            if selected_share is not None and universe_share
            else None
        )
        selected_market_caps = group.loc[group["selected"], "market_cap"]
        exposure_rows.append(
            {
                "date": signal_date.date().isoformat(),
                "size_bucket": int(size_bucket),
                "quantile_count": quantile_count,
                "universe_count": universe_count,
                "selected_count": selected_count,
                "universe_share": universe_share,
                "selected_share": selected_share,
                "selection_lift": selection_lift,
                "universe_market_cap_median": group["market_cap"].median(),
                "selected_market_cap_median": (
                    selected_market_caps.median()
                    if not selected_market_caps.empty
                    else None
                ),
                "valid_market_cap_count": valid_count,
                "selected_valid_market_cap_count": selected_valid_count,
                "market_cap_coverage_rate": valid_count
                / int(candidate_counts.loc[signal_date]),
                "selected_market_cap_coverage_rate": (
                    selected_valid_count / int(selected_counts.loc[signal_date])
                    if selected_counts.get(signal_date, 0)
                    else None
                ),
            }
        )

    exposure = pd.DataFrame(exposure_rows, columns=SIZE_EXPOSURE_COLUMNS)
    summary = _summarize_size_exposure(exposure, quantile_count)
    coverage_rows = []
    for signal_date, candidate_count in candidate_counts.items():
        valid_count = int(valid_counts.get(signal_date, 0))
        selected_count = int(selected_counts.get(signal_date, 0))
        selected_valid_count = int(selected_valid_counts.get(signal_date, 0))
        coverage_rows.append(
            {
                "date": signal_date.date().isoformat(),
                "candidate_count": int(candidate_count),
                "valid_market_cap_count": valid_count,
                "market_cap_coverage_rate": valid_count / int(candidate_count),
                "selected_count": selected_count,
                "selected_valid_market_cap_count": selected_valid_count,
                "selected_market_cap_coverage_rate": (
                    selected_valid_count / selected_count if selected_count else None
                ),
            }
        )
    coverage = pd.DataFrame(
        coverage_rows,
        columns=SIZE_EXPOSURE_COVERAGE_COLUMNS,
    )
    return SizeExposureDiagnosticReport(
        exposure=exposure,
        summary=summary,
        coverage=coverage,
    )


def write_size_exposure_diagnostic_report(
    report: SizeExposureDiagnosticReport,
    output_dir: str | Path,
    *,
    parameters: dict,
) -> Path:
    """Write CSV and Markdown artifacts for a size exposure run."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    (output_path / "parameters.json").write_text(
        json.dumps(parameters, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    report.exposure.to_csv(
        output_path / "size_exposure.csv", index=False, columns=SIZE_EXPOSURE_COLUMNS
    )
    report.summary.to_csv(
        output_path / "size_exposure_summary.csv",
        index=False,
        columns=SIZE_EXPOSURE_SUMMARY_COLUMNS,
    )
    report.coverage.to_csv(
        output_path / "size_exposure_coverage.csv",
        index=False,
        columns=SIZE_EXPOSURE_COVERAGE_COLUMNS,
    )
    (output_path / "summary.md").write_text(
        _build_size_exposure_summary(report, parameters),
        encoding="utf-8",
    )
    return output_path


def _normalize_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol", "market_cap"}
    missing = required - set(candidates.columns)
    if missing:
        raise ValueError("规模诊断候选数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = candidates.loc[:, ["date", "symbol", "market_cap"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    if normalized["date"].isna().any() or normalized["symbol"].isna().any():
        raise ValueError("规模诊断候选数据的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("规模诊断候选数据不能包含重复的 date/symbol")
    return normalized


def _normalize_targets(targets: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol"}
    missing = required - set(targets.columns)
    if missing:
        raise ValueError("规模诊断目标数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = targets.loc[:, ["date", "symbol"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    if normalized["date"].isna().any() or normalized["symbol"].isna().any():
        raise ValueError("规模诊断目标数据的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("规模诊断目标数据不能包含重复的 date/symbol")
    return normalized


def _summarize_size_exposure(
    exposure: pd.DataFrame, quantile_count: int
) -> pd.DataFrame:
    if exposure.empty:
        return pd.DataFrame(columns=SIZE_EXPOSURE_SUMMARY_COLUMNS)
    summary = (
        exposure.groupby("size_bucket", sort=True)
        .agg(
            signal_date_count=("date", "nunique"),
            mean_universe_share=("universe_share", "mean"),
            mean_selected_share=("selected_share", "mean"),
            mean_selection_lift=("selection_lift", "mean"),
            median_universe_market_cap=("universe_market_cap_median", "median"),
            median_selected_market_cap=("selected_market_cap_median", "median"),
            mean_market_cap_coverage_rate=("market_cap_coverage_rate", "mean"),
            mean_selected_market_cap_coverage_rate=(
                "selected_market_cap_coverage_rate",
                "mean",
            ),
        )
        .reset_index()
    )
    summary["quantile_count"] = quantile_count
    summary["mean_share_difference"] = (
        summary["mean_selected_share"] - summary["mean_universe_share"]
    )
    return summary.loc[:, SIZE_EXPOSURE_SUMMARY_COLUMNS]


def _build_size_exposure_summary(
    report: SizeExposureDiagnosticReport, parameters: dict
) -> str:
    coverage = report.coverage
    lines = [
        "# 因子实验规模暴露诊断",
        "",
        "本报告比较每个信号日可选股票池与最终入选持仓的点时市值分布；它是暴露审计，不是收益因果归因。",
        "",
        "## 1. 运行信息",
        "",
    ]
    _append_table(
        lines,
        ["项目", "内容"],
        [
            ("生成时间", datetime.now().astimezone().isoformat(timespec="seconds")),
            ("策略配置", parameters.get("backtest_config_path", "—")),
            ("信号日数量", coverage["date"].nunique() if not coverage.empty else 0),
            (
                "规模分组数",
                parameters.get("quantile_count", "—"),
            ),
            ("市值口径", "v_daily_valuation.market_cap，信号日点时值"),
        ],
    )
    lines.extend(["", "## 2. 市值覆盖与规模暴露", ""])
    if coverage.empty:
        lines.append("没有可生成的规模覆盖记录。")
    else:
        _append_table(
            lines,
            [
                "指标",
                "均值",
                "最小值",
                "最大值",
            ],
            [
                (
                    "可选池市值覆盖率",
                    _format_percent(coverage["market_cap_coverage_rate"].mean()),
                    _format_percent(coverage["market_cap_coverage_rate"].min()),
                    _format_percent(coverage["market_cap_coverage_rate"].max()),
                ),
                (
                    "入选持仓市值覆盖率",
                    _format_percent(
                        coverage["selected_market_cap_coverage_rate"].mean()
                    ),
                    _format_percent(
                        coverage["selected_market_cap_coverage_rate"].min()
                    ),
                    _format_percent(
                        coverage["selected_market_cap_coverage_rate"].max()
                    ),
                ),
            ],
        )
    lines.extend(["", "### 各规模组汇总", ""])
    _append_table(
        lines,
        [
            "规模组",
            "信号日数",
            "平均可选占比",
            "平均入选占比",
            "平均选择提升",
            "平均占比差",
        ],
        [
            (
                int(row["size_bucket"]),
                int(row["signal_date_count"]),
                _format_percent(row["mean_universe_share"]),
                _format_percent(row["mean_selected_share"]),
                _format_metric(row["mean_selection_lift"]),
                _format_percent(row["mean_share_difference"]),
            )
            for _, row in report.summary.iterrows()
        ],
    )
    lines.extend(["", "## 3. 研究限制与提示", ""])
    lines.append(
        "规模组 1 表示当日较小市值组，规模组 N 表示当日较大市值组；选择提升等于入选占比除以可选占比。"
    )
    warning_rows = _size_exposure_warnings(report.summary)
    if warning_rows:
        lines.extend(["", "规模暴露提示："])
        lines.extend(f"- {warning}" for warning in warning_rows)
    else:
        lines.extend(["", "本次没有触发预设的规模选择提升提示。"])
    lines.extend(
        [
            "",
            "当前 stocks.industry 只有未版本化的元数据快照，不能用于本次历史区间的点时行业暴露或行业中性结论。后续需要补齐带生效日期的历史行业数据后再实现行业诊断。",
            "规模暴露与收益之间不构成因果证明；本报告不改变策略目标、训练权重、成交或回测结果。",
            "",
            "## 4. 审计文件",
            "",
        ]
    )
    _append_table(
        lines,
        ["文件", "内容"],
        [
            ("`parameters.json`", "配置和点时口径"),
            ("`size_exposure.csv`", "逐信号日、逐规模组暴露"),
            ("`size_exposure_summary.csv`", "跨信号日规模组汇总"),
            ("`size_exposure_coverage.csv`", "逐信号日市值覆盖率"),
        ],
    )
    return "\n".join(lines) + "\n"


def _size_exposure_warnings(summary: pd.DataFrame) -> list[str]:
    warnings = []
    for _, row in summary.iterrows():
        lift = row.get("mean_selection_lift")
        if pd.isna(lift):
            continue
        bucket = int(row["size_bucket"])
        if lift >= SIZE_EXPOSURE_LIFT_WARNING_THRESHOLD:
            warnings.append(
                f"规模组 {bucket} 平均入选提升为 {_format_metric(lift)}，持仓相对可选池明显偏多。"
            )
        elif lift <= SIZE_EXPOSURE_AVOIDANCE_WARNING_THRESHOLD:
            warnings.append(
                f"规模组 {bucket} 平均入选提升为 {_format_metric(lift)}，持仓相对可选池明显偏少。"
            )
    return warnings


def _append_table(lines: list[str], headers, rows) -> None:
    lines.append("| " + " | ".join(str(header) for header in headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(_format_value(value) for value in row) + " |")


def _format_value(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return str(value).replace("|", "\\|").replace("\n", " ")


def _format_metric(value) -> str:
    return "—" if pd.isna(value) else f"{float(value):.4f}"


def _format_percent(value) -> str:
    return "—" if pd.isna(value) else f"{float(value):.1%}"


def _validate_quantile_count(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 2:
        raise ValueError("quantile_count 必须是大于等于 2 的整数")
    return value
