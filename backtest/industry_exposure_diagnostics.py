"""Point-in-time industry coverage and exposure diagnostics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

INDUSTRY_EXPOSURE_COLUMNS = (
    "date",
    "industry_code",
    "universe_count",
    "selected_count",
    "universe_share",
    "selected_share",
    "selection_lift",
    "classified_universe_count",
    "classified_selected_count",
    "industry_coverage_rate",
    "selected_industry_coverage_rate",
)
INDUSTRY_EXPOSURE_SUMMARY_COLUMNS = (
    "industry_code",
    "signal_date_count",
    "universe_count",
    "selected_count",
    "mean_universe_share",
    "mean_selected_share",
    "mean_selection_lift",
    "mean_share_difference",
)
INDUSTRY_EXPOSURE_COVERAGE_COLUMNS = (
    "date",
    "candidate_count",
    "classified_candidate_count",
    "missing_candidate_count",
    "industry_coverage_rate",
    "selected_count",
    "classified_selected_count",
    "missing_selected_count",
    "selected_industry_coverage_rate",
)
INDUSTRY_COVERAGE_WARNING_THRESHOLD = 0.95
INDUSTRY_SMALL_SAMPLE_COUNT = 100


@dataclass(frozen=True)
class IndustryExposureDiagnosticReport:
    """The per-signal industry exposure and coverage tables."""

    exposure: pd.DataFrame
    summary: pd.DataFrame
    coverage: pd.DataFrame


def calculate_industry_exposure_diagnostics(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
) -> IndustryExposureDiagnosticReport:
    """Compare selected holdings with the point-in-time industry universe."""

    normalized_candidates = _normalize_candidates(candidates)
    normalized_targets = _normalize_targets(targets)
    if normalized_candidates.empty:
        raise ValueError("行业暴露诊断没有可选股票池")
    target_membership = normalized_targets.merge(
        normalized_candidates.loc[:, ["date", "symbol"]],
        on=["date", "symbol"],
        how="left",
        indicator=True,
    )
    if (target_membership["_merge"] != "both").any():
        raise ValueError("行业暴露诊断目标必须属于对应信号日的可选股票池")

    candidate_counts = normalized_candidates.groupby("date", sort=True).size()
    selected_counts = normalized_targets.groupby("date", sort=True).size()
    merged = normalized_candidates.merge(
        normalized_targets.loc[:, ["date", "symbol"]].assign(selected=True),
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    merged["selected"] = merged["selected"].fillna(False).astype(bool)
    classified = merged.loc[merged["industry_code"].notna()].copy()
    classified_counts = classified.groupby("date", sort=True).size()
    classified_selected_counts = (
        classified.loc[classified["selected"]].groupby("date", sort=True).size()
    )

    exposure_rows = []
    for (signal_date, industry_code), group in classified.groupby(
        ["date", "industry_code"], sort=True
    ):
        classified_count = int(classified_counts.loc[signal_date])
        classified_selected_count = int(classified_selected_counts.get(signal_date, 0))
        universe_count = len(group)
        selected_count = int(group["selected"].sum())
        universe_share = universe_count / classified_count
        selected_share = (
            selected_count / classified_selected_count
            if classified_selected_count
            else None
        )
        selection_lift = (
            selected_share / universe_share
            if selected_share is not None and universe_share
            else None
        )
        exposure_rows.append(
            {
                "date": signal_date.date().isoformat(),
                "industry_code": industry_code,
                "universe_count": universe_count,
                "selected_count": selected_count,
                "universe_share": universe_share,
                "selected_share": selected_share,
                "selection_lift": selection_lift,
                "classified_universe_count": classified_count,
                "classified_selected_count": classified_selected_count,
                "industry_coverage_rate": classified_count
                / int(candidate_counts.loc[signal_date]),
                "selected_industry_coverage_rate": (
                    classified_selected_count / int(selected_counts.loc[signal_date])
                    if selected_counts.get(signal_date, 0)
                    else None
                ),
            }
        )

    exposure = pd.DataFrame(exposure_rows, columns=INDUSTRY_EXPOSURE_COLUMNS)
    summary = _summarize_industry_exposure(exposure)
    coverage_rows = []
    for signal_date, candidate_count in candidate_counts.items():
        classified_count = int(classified_counts.get(signal_date, 0))
        selected_count = int(selected_counts.get(signal_date, 0))
        classified_selected_count = int(classified_selected_counts.get(signal_date, 0))
        coverage_rows.append(
            {
                "date": signal_date.date().isoformat(),
                "candidate_count": int(candidate_count),
                "classified_candidate_count": classified_count,
                "missing_candidate_count": int(candidate_count) - classified_count,
                "industry_coverage_rate": classified_count / int(candidate_count),
                "selected_count": selected_count,
                "classified_selected_count": classified_selected_count,
                "missing_selected_count": selected_count - classified_selected_count,
                "selected_industry_coverage_rate": (
                    classified_selected_count / selected_count
                    if selected_count
                    else None
                ),
            }
        )
    coverage = pd.DataFrame(
        coverage_rows,
        columns=INDUSTRY_EXPOSURE_COVERAGE_COLUMNS,
    )
    return IndustryExposureDiagnosticReport(
        exposure=exposure,
        summary=summary,
        coverage=coverage,
    )


def write_industry_exposure_diagnostic_report(
    report: IndustryExposureDiagnosticReport,
    output_dir: str | Path,
    *,
    parameters: dict,
) -> Path:
    """Write CSV and Markdown artifacts for an industry exposure run."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    (output_path / "parameters.json").write_text(
        json.dumps(parameters, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    report.exposure.to_csv(
        output_path / "industry_exposure.csv",
        index=False,
        columns=INDUSTRY_EXPOSURE_COLUMNS,
    )
    report.summary.to_csv(
        output_path / "industry_exposure_summary.csv",
        index=False,
        columns=INDUSTRY_EXPOSURE_SUMMARY_COLUMNS,
    )
    report.coverage.to_csv(
        output_path / "industry_exposure_coverage.csv",
        index=False,
        columns=INDUSTRY_EXPOSURE_COVERAGE_COLUMNS,
    )
    (output_path / "summary.md").write_text(
        _build_industry_exposure_summary(report, parameters),
        encoding="utf-8",
    )
    return output_path


def _normalize_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol", "industry_code"}
    missing = required - set(candidates.columns)
    if missing:
        raise ValueError("行业诊断候选数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = candidates.loc[:, ["date", "symbol", "industry_code"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    normalized["industry_code"] = (
        normalized["industry_code"].astype("string").str.strip()
    )
    normalized.loc[normalized["industry_code"] == "", "industry_code"] = pd.NA
    if normalized["date"].isna().any() or normalized["symbol"].isna().any():
        raise ValueError("行业诊断候选数据的 date 和 symbol 不能为空")
    invalid_codes = (
        normalized["industry_code"]
        .dropna()
        .loc[lambda values: ~values.str.fullmatch(r"\d{6}", na=False)]
    )
    if not invalid_codes.empty:
        raise ValueError("行业诊断候选数据包含非法行业代码")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("行业诊断候选数据不能包含重复的 date/symbol")
    return normalized


def _normalize_targets(targets: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol"}
    missing = required - set(targets.columns)
    if missing:
        raise ValueError("行业诊断目标数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = targets.loc[:, ["date", "symbol"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    if normalized["date"].isna().any() or normalized["symbol"].isna().any():
        raise ValueError("行业诊断目标数据的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("行业诊断目标数据不能包含重复的 date/symbol")
    return normalized


def _summarize_industry_exposure(exposure: pd.DataFrame) -> pd.DataFrame:
    if exposure.empty:
        return pd.DataFrame(columns=INDUSTRY_EXPOSURE_SUMMARY_COLUMNS)
    summary = (
        exposure.groupby("industry_code", sort=True)
        .agg(
            signal_date_count=("date", "nunique"),
            universe_count=("universe_count", "sum"),
            selected_count=("selected_count", "sum"),
            mean_universe_share=("universe_share", "mean"),
            mean_selected_share=("selected_share", "mean"),
            mean_selection_lift=("selection_lift", "mean"),
        )
        .reset_index()
    )
    summary["mean_share_difference"] = (
        summary["mean_selected_share"] - summary["mean_universe_share"]
    )
    return summary.loc[:, INDUSTRY_EXPOSURE_SUMMARY_COLUMNS]


def _build_industry_exposure_summary(
    report: IndustryExposureDiagnosticReport,
    parameters: dict,
) -> str:
    coverage = report.coverage
    lines = [
        "# 因子实验历史行业覆盖与暴露诊断",
        "",
        "本报告使用申万历史行业分类的生效日期做 ASOF 对齐，比较候选池与入选持仓的行业覆盖和选择暴露；它不改变策略行为，也不是收益因果归因。",
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
            ("行业数据资产", "industry_classification_sw"),
            ("ASOF 字段", "effective_date"),
        ],
    )
    lines.extend(["", "## 2. 行业覆盖率", ""])
    if coverage.empty:
        lines.append("没有可生成的行业覆盖记录。")
    else:
        _append_table(
            lines,
            ["指标", "均值", "最小值", "最大值"],
            [
                (
                    "候选池行业覆盖率",
                    _format_percent(coverage["industry_coverage_rate"].mean()),
                    _format_percent(coverage["industry_coverage_rate"].min()),
                    _format_percent(coverage["industry_coverage_rate"].max()),
                ),
                (
                    "入选持仓行业覆盖率",
                    _format_percent(coverage["selected_industry_coverage_rate"].mean()),
                    _format_percent(coverage["selected_industry_coverage_rate"].min()),
                    _format_percent(coverage["selected_industry_coverage_rate"].max()),
                ),
            ],
        )
        warnings = _coverage_warnings(coverage)
        if warnings:
            lines.extend(["", "行业覆盖率提示："])
            lines.extend(f"- {warning}" for warning in warnings)
    lines.extend(["", "## 3. 行业选择暴露", ""])
    if report.summary.empty:
        lines.append("没有已分类行业记录，无法生成行业选择暴露。")
    else:
        _append_table(
            lines,
            [
                "行业代码",
                "信号日数",
                "候选数",
                "入选数",
                "平均候选占比",
                "平均入选占比",
                "平均占比差",
                "平均选择提升",
            ],
            [
                (
                    row["industry_code"],
                    int(row["signal_date_count"]),
                    int(row["universe_count"]),
                    int(row["selected_count"]),
                    _format_percent(row["mean_universe_share"]),
                    _format_percent(row["mean_selected_share"]),
                    _format_percent(row["mean_share_difference"]),
                    _format_metric(row["mean_selection_lift"]),
                )
                for _, row in report.summary.sort_values(
                    ["mean_share_difference", "industry_code"],
                    ascending=[False, True],
                    na_position="last",
                )
                .head(20)
                .iterrows()
            ],
        )
        lines.append("按平均占比差展示前 20 个行业代码，完整结果见 CSV。")
        small_sample_count = int(
            (report.summary["universe_count"] < INDUSTRY_SMALL_SAMPLE_COUNT).sum()
        )
        if small_sample_count:
            lines.append(
                f"提示：{small_sample_count} 个行业代码累计候选数少于 {INDUSTRY_SMALL_SAMPLE_COUNT}，其选择提升比率可能不稳定，应结合候选数和占比差解读。"
            )
    lines.extend(
        [
            "",
            "## 4. 研究限制",
            "",
            "行业代码来自申万历史分类，当前资产未提供稳定的行业名称映射，因此报告不显示行业名称。未匹配行业不使用 stocks.industry 当前快照回填。行业选择暴露不构成行业收益或策略收益的因果证明，本阶段不实施行业中性化。",
            "",
            "## 5. 审计文件",
            "",
        ]
    )
    _append_table(
        lines,
        ["文件", "内容"],
        [
            ("`parameters.json`", "配置和点时口径"),
            ("`industry_exposure.csv`", "逐信号日、逐行业代码暴露"),
            ("`industry_exposure_summary.csv`", "跨信号日行业代码汇总"),
            ("`industry_exposure_coverage.csv`", "逐信号日候选池/持仓覆盖率"),
        ],
    )
    return "\n".join(lines) + "\n"


def _coverage_warnings(coverage: pd.DataFrame) -> list[str]:
    warnings = []
    for column, label in (
        ("industry_coverage_rate", "候选池"),
        ("selected_industry_coverage_rate", "入选持仓"),
    ):
        minimum = coverage[column].min()
        if pd.notna(minimum) and minimum < INDUSTRY_COVERAGE_WARNING_THRESHOLD:
            warnings.append(
                f"{label}行业覆盖率最低为 {_format_percent(minimum)}，低于 {_format_percent(INDUSTRY_COVERAGE_WARNING_THRESHOLD)}，不宜直接实施行业约束。"
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
