"""Research-only industry and size neutralization comparison diagnostics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.exposure_diagnostics import (
    SIZE_EXPOSURE_COLUMNS,
    calculate_size_exposure_diagnostics,
)
from backtest.industry_exposure_diagnostics import (
    INDUSTRY_EXPOSURE_COLUMNS,
    calculate_industry_exposure_diagnostics,
)

NEUTRALIZATION_MODES = ("industry", "size", "industry_size")
NEUTRALIZATION_SUMMARY_COLUMNS = (
    "mode",
    "signal_date_count",
    "successful_signal_date_count",
    "failed_signal_date_count",
    "mean_control_coverage_rate",
    "min_control_coverage_rate",
    "mean_target_overlap_rate",
    "mean_abs_industry_share_difference",
    "max_abs_industry_share_difference",
    "mean_abs_size_share_difference",
    "max_abs_size_share_difference",
)
NEUTRALIZATION_COVERAGE_COLUMNS = (
    "mode",
    "date",
    "candidate_count",
    "valid_control_count",
    "control_coverage_rate",
    "holding_count",
    "selected_count",
    "status",
    "failure_reason",
)
NEUTRALIZATION_OVERLAP_COLUMNS = (
    "mode",
    "date",
    "baseline_target_count",
    "neutralized_target_count",
    "overlap_count",
    "overlap_rate",
)
NEUTRALIZATION_INDUSTRY_EXPOSURE_COLUMNS = (
    "mode",
    *INDUSTRY_EXPOSURE_COLUMNS,
)
NEUTRALIZATION_SIZE_EXPOSURE_COLUMNS = (
    "mode",
    *SIZE_EXPOSURE_COLUMNS,
)


@dataclass(frozen=True)
class NeutralizationDiagnosticReport:
    """Comparison tables for baseline and residualized targets."""

    summary: pd.DataFrame
    coverage: pd.DataFrame
    target_overlap: pd.DataFrame
    industry_exposure: pd.DataFrame
    size_exposure: pd.DataFrame


def calculate_neutralization_diagnostics(
    candidates: pd.DataFrame,
    baseline_targets: pd.DataFrame,
    *,
    holding_count: int,
    quantile_count: int = 5,
) -> NeutralizationDiagnosticReport:
    """Compare baseline targets with industry/size residualized targets."""

    if isinstance(holding_count, bool) or not isinstance(holding_count, int):
        raise ValueError("holding_count 必须是正整数")
    if holding_count <= 0:
        raise ValueError("holding_count 必须是正整数")
    if isinstance(quantile_count, bool) or not isinstance(quantile_count, int):
        raise ValueError("quantile_count 必须是正整数")
    if quantile_count <= 0:
        raise ValueError("quantile_count 必须是正整数")
    normalized_candidates = _normalize_candidates(candidates)
    normalized_baseline = _normalize_targets(baseline_targets)
    if normalized_candidates.empty:
        raise ValueError("中性化诊断没有可选股票池")
    _validate_target_membership(normalized_candidates, normalized_baseline)

    candidate_counts = normalized_candidates.groupby("date", sort=True).size()
    coverage_rows = []
    overlap_rows = []
    summary_rows = []
    industry_exposure_frames = []
    size_exposure_frames = []

    baseline_coverage = _build_baseline_coverage(
        candidate_counts,
        normalized_baseline,
    )
    coverage_rows.extend(baseline_coverage)
    overlap_rows.extend(
        _build_overlap_rows("baseline", normalized_baseline, normalized_baseline)
    )
    baseline_metrics, industry_exposure, size_exposure = _calculate_exposure_metrics(
        normalized_candidates,
        normalized_baseline,
        quantile_count,
    )
    industry_exposure_frames.append(
        _add_mode_column(
            industry_exposure,
            "baseline",
            NEUTRALIZATION_INDUSTRY_EXPOSURE_COLUMNS,
        )
    )
    size_exposure_frames.append(
        _add_mode_column(
            size_exposure,
            "baseline",
            NEUTRALIZATION_SIZE_EXPOSURE_COLUMNS,
        )
    )
    summary_rows.append(
        _build_mode_summary(
            "baseline",
            baseline_coverage,
            overlap_rows,
            baseline_metrics,
        )
    )

    for mode in NEUTRALIZATION_MODES:
        targets, mode_coverage = _build_neutralized_targets(
            normalized_candidates,
            mode,
            holding_count,
        )
        coverage_rows.extend(mode_coverage)
        overlap = _build_overlap_rows(mode, normalized_baseline, targets)
        overlap_rows.extend(overlap)
        metrics, industry_exposure, size_exposure = _calculate_exposure_metrics(
            normalized_candidates,
            targets,
            quantile_count,
        )
        industry_exposure_frames.append(
            _add_mode_column(
                industry_exposure,
                mode,
                NEUTRALIZATION_INDUSTRY_EXPOSURE_COLUMNS,
            )
        )
        size_exposure_frames.append(
            _add_mode_column(
                size_exposure,
                mode,
                NEUTRALIZATION_SIZE_EXPOSURE_COLUMNS,
            )
        )
        summary_rows.append(_build_mode_summary(mode, mode_coverage, overlap, metrics))

    summary = pd.DataFrame(summary_rows, columns=NEUTRALIZATION_SUMMARY_COLUMNS)
    coverage = pd.DataFrame(coverage_rows, columns=NEUTRALIZATION_COVERAGE_COLUMNS)
    target_overlap = pd.DataFrame(
        overlap_rows,
        columns=NEUTRALIZATION_OVERLAP_COLUMNS,
    )
    industry_exposure = pd.concat(industry_exposure_frames, ignore_index=True)
    size_exposure = pd.concat(size_exposure_frames, ignore_index=True)
    return NeutralizationDiagnosticReport(
        summary,
        coverage,
        target_overlap,
        industry_exposure,
        size_exposure,
    )


def write_neutralization_diagnostic_report(
    report: NeutralizationDiagnosticReport,
    output_dir: str | Path,
    *,
    parameters: dict,
) -> Path:
    """Write neutralization comparison artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    (output_path / "parameters.json").write_text(
        json.dumps(parameters, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    report.summary.to_csv(
        output_path / "neutralization_summary.csv",
        index=False,
        columns=NEUTRALIZATION_SUMMARY_COLUMNS,
    )
    report.coverage.to_csv(
        output_path / "neutralization_coverage.csv",
        index=False,
        columns=NEUTRALIZATION_COVERAGE_COLUMNS,
    )
    report.target_overlap.to_csv(
        output_path / "neutralization_target_overlap.csv",
        index=False,
        columns=NEUTRALIZATION_OVERLAP_COLUMNS,
    )
    report.industry_exposure.to_csv(
        output_path / "neutralization_industry_exposure.csv",
        index=False,
        columns=NEUTRALIZATION_INDUSTRY_EXPOSURE_COLUMNS,
    )
    report.size_exposure.to_csv(
        output_path / "neutralization_size_exposure.csv",
        index=False,
        columns=NEUTRALIZATION_SIZE_EXPOSURE_COLUMNS,
    )
    (output_path / "summary.md").write_text(
        _build_summary(report, parameters),
        encoding="utf-8",
    )
    return output_path


def _normalize_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol", "score", "industry_code", "market_cap"}
    missing = required - set(candidates.columns)
    if missing:
        raise ValueError("中性化候选数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = candidates.loc[
        :, ["date", "symbol", "score", "industry_code", "market_cap"]
    ].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    normalized["score"] = pd.to_numeric(normalized["score"], errors="coerce")
    normalized["market_cap"] = pd.to_numeric(normalized["market_cap"], errors="coerce")
    normalized["industry_code"] = (
        normalized["industry_code"].astype("string").str.strip()
    )
    normalized.loc[normalized["industry_code"] == "", "industry_code"] = pd.NA
    if (
        normalized["date"].isna().any()
        or normalized["symbol"].isna().any()
        or normalized["symbol"].eq("").any()
        or normalized["score"].isna().any()
        or ~np.isfinite(normalized["score"]).all()
    ):
        raise ValueError("中性化候选数据的 date、symbol 和 score 必须是有效值")
    invalid_codes = (
        normalized["industry_code"]
        .dropna()
        .loc[lambda values: ~values.str.fullmatch(r"\d{6}", na=False)]
    )
    if not invalid_codes.empty:
        raise ValueError("中性化候选数据包含非法行业代码")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("中性化候选数据不能包含重复的 date/symbol")
    return normalized


def _normalize_targets(targets: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol"}
    missing = required - set(targets.columns)
    if missing:
        raise ValueError("中性化目标数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = targets.loc[:, ["date", "symbol"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    if (
        normalized["date"].isna().any()
        or normalized["symbol"].isna().any()
        or normalized["symbol"].eq("").any()
    ):
        raise ValueError("中性化目标数据的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("中性化目标数据不能包含重复的 date/symbol")
    return normalized


def _validate_target_membership(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
) -> None:
    membership = targets.merge(
        candidates.loc[:, ["date", "symbol"]],
        on=["date", "symbol"],
        how="left",
        indicator=True,
    )
    if (membership["_merge"] != "both").any():
        raise ValueError("中性化目标必须属于对应信号日的可选股票池")


def _build_baseline_coverage(
    candidate_counts: pd.Series,
    baseline_targets: pd.DataFrame,
) -> list[dict]:
    selected_counts = baseline_targets.groupby("date", sort=True).size()
    return [
        {
            "mode": "baseline",
            "date": signal_date.date().isoformat(),
            "candidate_count": int(candidate_count),
            "valid_control_count": int(candidate_count),
            "control_coverage_rate": 1.0,
            "holding_count": None,
            "selected_count": int(selected_counts.get(signal_date, 0)),
            "status": "success",
            "failure_reason": None,
        }
        for signal_date, candidate_count in candidate_counts.items()
    ]


def _build_neutralized_targets(
    candidates: pd.DataFrame,
    mode: str,
    holding_count: int,
) -> tuple[pd.DataFrame, list[dict]]:
    _validate_mode(mode)
    target_rows = []
    coverage_rows = []
    for signal_date, group in candidates.groupby("date", sort=True):
        valid_mask = _control_mask(group, mode)
        valid = group.loc[valid_mask].copy()
        candidate_count = len(group)
        valid_count = len(valid)
        coverage = valid_count / candidate_count if candidate_count else None
        row = {
            "mode": mode,
            "date": signal_date.date().isoformat(),
            "candidate_count": candidate_count,
            "valid_control_count": valid_count,
            "control_coverage_rate": coverage,
            "holding_count": holding_count,
            "selected_count": 0,
            "status": "success",
            "failure_reason": None,
        }
        if valid_count < holding_count:
            row["status"] = "failed"
            row["failure_reason"] = (
                f"有效控制变量候选 {valid_count} 少于持仓数 {holding_count}"
            )
            coverage_rows.append(row)
            continue

        valid["neutralized_score"] = _residualize_scores(valid, mode)
        valid = valid.sort_values(
            ["neutralized_score", "symbol"],
            ascending=[False, True],
            kind="mergesort",
        )
        selected = valid.head(holding_count).copy()
        selected["rank"] = range(1, len(selected) + 1)
        selected["score"] = selected["neutralized_score"]
        selected["target_weight"] = 1 / len(selected)
        target_rows.extend(
            selected.loc[
                :, ["date", "symbol", "score", "rank", "target_weight"]
            ].to_dict("records")
        )
        row["selected_count"] = len(selected)
        coverage_rows.append(row)

    targets = pd.DataFrame(
        target_rows,
        columns=["date", "symbol", "score", "rank", "target_weight"],
    )
    if not targets.empty:
        targets["date"] = pd.to_datetime(targets["date"])
    return targets, coverage_rows


def _control_mask(group: pd.DataFrame, mode: str) -> pd.Series:
    valid_industry = group["industry_code"].notna()
    valid_size = (
        group["market_cap"].notna()
        & np.isfinite(group["market_cap"])
        & group["market_cap"].gt(0)
    )
    if mode == "industry":
        return valid_industry
    if mode == "size":
        return valid_size
    return valid_industry & valid_size


def _residualize_scores(group: pd.DataFrame, mode: str) -> pd.Series:
    design_parts = []
    if mode in {"industry", "industry_size"}:
        dummies = pd.get_dummies(group["industry_code"], dtype=float)
        design_parts.append(dummies.to_numpy())
    if mode == "size":
        design_parts.append(np.ones((len(group), 1)))
    if mode in {"size", "industry_size"}:
        design_parts.append(np.log(group["market_cap"].to_numpy())[:, None])
    design = np.column_stack(design_parts)
    scores = group["score"].to_numpy(dtype=float)
    coefficients, _, _, _ = np.linalg.lstsq(design, scores, rcond=None)
    return pd.Series(scores - design @ coefficients, index=group.index)


def _target_sets(targets: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
    return {
        signal_date: set(group["symbol"])
        for signal_date, group in targets.groupby("date", sort=True)
    }


def _build_overlap_rows(
    mode: str,
    baseline_targets: pd.DataFrame,
    targets: pd.DataFrame,
) -> list[dict]:
    baseline_sets = _target_sets(baseline_targets)
    target_sets = _target_sets(targets)
    rows = []
    for signal_date, baseline_set in baseline_sets.items():
        current_set = target_sets.get(signal_date, set())
        rows.append(
            {
                "mode": mode,
                "date": signal_date.date().isoformat(),
                "baseline_target_count": len(baseline_set),
                "neutralized_target_count": len(current_set),
                "overlap_count": len(baseline_set & current_set),
                "overlap_rate": (
                    len(baseline_set & current_set) / len(baseline_set)
                    if baseline_set and current_set
                    else None
                ),
            }
        )
    return rows


def _calculate_exposure_metrics(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
    quantile_count: int,
) -> tuple[dict[str, float | None], pd.DataFrame, pd.DataFrame]:
    if targets.empty:
        return (
            {
                "mean_abs_industry_share_difference": None,
                "max_abs_industry_share_difference": None,
                "mean_abs_size_share_difference": None,
                "max_abs_size_share_difference": None,
            },
            pd.DataFrame(columns=INDUSTRY_EXPOSURE_COLUMNS),
            pd.DataFrame(columns=SIZE_EXPOSURE_COLUMNS),
        )
    dates = targets["date"].drop_duplicates()
    scoped_candidates = candidates[candidates["date"].isin(dates)]
    industry_report = calculate_industry_exposure_diagnostics(
        scoped_candidates,
        targets,
    )
    size_report = calculate_size_exposure_diagnostics(
        scoped_candidates,
        targets,
        quantile_count=quantile_count,
    )
    industry_differences = (
        industry_report.exposure["selected_share"]
        - industry_report.exposure["universe_share"]
    ).abs()
    size_differences = (
        size_report.exposure["selected_share"] - size_report.exposure["universe_share"]
    ).abs()
    return (
        {
            "mean_abs_industry_share_difference": (
                float(industry_differences.mean())
                if not industry_differences.empty
                else None
            ),
            "max_abs_industry_share_difference": (
                float(industry_differences.max())
                if not industry_differences.empty
                else None
            ),
            "mean_abs_size_share_difference": (
                float(size_differences.mean()) if not size_differences.empty else None
            ),
            "max_abs_size_share_difference": (
                float(size_differences.max()) if not size_differences.empty else None
            ),
        },
        industry_report.exposure,
        size_report.exposure,
    )


def _add_mode_column(
    exposure: pd.DataFrame,
    mode: str,
    columns: tuple[str, ...],
) -> pd.DataFrame:
    if exposure.empty:
        return pd.DataFrame(columns=columns)
    return exposure.assign(mode=mode).loc[:, columns]


def _build_mode_summary(
    mode: str,
    coverage_rows: list[dict],
    overlap_rows: list[dict],
    metrics: dict[str, float | None],
) -> dict:
    coverage = pd.DataFrame(coverage_rows)
    successful = coverage[coverage["status"] == "success"]
    overlap = pd.DataFrame(overlap_rows)
    return {
        "mode": mode,
        "signal_date_count": len(coverage),
        "successful_signal_date_count": len(successful),
        "failed_signal_date_count": int((coverage["status"] == "failed").sum()),
        "mean_control_coverage_rate": coverage["control_coverage_rate"].mean(),
        "min_control_coverage_rate": coverage["control_coverage_rate"].min(),
        "mean_target_overlap_rate": overlap["overlap_rate"].mean(),
        **metrics,
    }


def _validate_mode(mode: str) -> None:
    if mode not in NEUTRALIZATION_MODES:
        raise ValueError(
            "不支持的中性化模式: "
            + str(mode)
            + "；可选: "
            + ", ".join(NEUTRALIZATION_MODES)
        )


def _build_summary(
    report: NeutralizationDiagnosticReport,
    parameters: dict,
) -> str:
    lines = [
        "# 因子实验行业/规模中性化对照诊断",
        "",
        "本报告比较基准综合评分与行业、规模及联合残差评分的目标变化和暴露变化；它是研究对照，不改变正式策略或收益结果。",
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
            ("中性化模式", ", ".join(NEUTRALIZATION_MODES)),
            ("控制变量", "industry_classification_sw ASOF + log(market_cap)"),
            (
                "暴露分母",
                "行业使用全候选池中已分类股票；规模使用全候选池中正且有限市值候选；选中占比使用有效目标",
            ),
        ],
    )
    lines.extend(["", "## 2. 模式对照", ""])
    _append_table(
        lines,
        [
            "模式",
            "成功信号日",
            "失败信号日",
            "平均控制覆盖率",
            "平均目标重合率",
            "平均行业占比差绝对值",
            "平均规模占比差绝对值",
        ],
        [
            (
                row["mode"],
                int(row["successful_signal_date_count"]),
                int(row["failed_signal_date_count"]),
                _format_percent(row["mean_control_coverage_rate"]),
                _format_percent(row["mean_target_overlap_rate"]),
                _format_percent(row["mean_abs_industry_share_difference"]),
                _format_percent(row["mean_abs_size_share_difference"]),
            )
            for _, row in report.summary.iterrows()
        ],
    )
    lines.extend(
        [
            "",
            "## 3. 解释边界",
            "",
            "中性化模式只在控制变量完整的候选池中选股；失败信号日不会回退到基准目标。目标重合率低表示选股顺序被明显改变，不等于样本外收益改善。行业和规模暴露下降也不等于因子信号更有效，是否接入正式策略需另行验证交易成本、收益、回撤和样本外稳定性。",
            "",
            "## 4. 审计文件",
            "",
        ]
    )
    _append_table(
        lines,
        ["文件", "内容"],
        [
            ("`parameters.json`", "配置和控制变量口径"),
            ("`neutralization_summary.csv`", "各模式信号日、覆盖率、重合率和暴露汇总"),
            ("`neutralization_coverage.csv`", "逐模式、逐信号日控制变量覆盖和失败原因"),
            ("`neutralization_target_overlap.csv`", "逐模式、逐信号日与基准目标重合"),
            (
                "`neutralization_industry_exposure.csv`",
                "逐模式、逐信号日、逐行业占比和选择提升",
            ),
            (
                "`neutralization_size_exposure.csv`",
                "逐模式、逐信号日、逐规模组占比和选择提升",
            ),
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
    return "—" if pd.isna(value) else f"{float(value):.1%}"
