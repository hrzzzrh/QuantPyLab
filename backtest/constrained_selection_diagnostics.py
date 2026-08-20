"""Research-only proportional industry and size quota diagnostics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.neutralization_diagnostics import (
    _normalize_candidates,
    _normalize_targets,
    _validate_target_membership,
)

CONSTRAINED_SELECTION_MODES = (
    "industry_quota",
    "size_quota",
    "industry_size_quota",
)
CONSTRAINT_SUMMARY_COLUMNS = (
    "mode",
    "signal_date_count",
    "successful_signal_date_count",
    "failed_signal_date_count",
    "mean_control_coverage_rate",
    "min_control_coverage_rate",
    "mean_target_overlap_rate",
    "mean_abs_share_difference",
    "max_abs_share_difference",
    "max_abs_quota_error",
)
CONSTRAINT_COVERAGE_COLUMNS = (
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
CONSTRAINT_OVERLAP_COLUMNS = (
    "mode",
    "date",
    "baseline_target_count",
    "constrained_target_count",
    "overlap_count",
    "overlap_rate",
)
CONSTRAINT_EXPOSURE_COLUMNS = (
    "mode",
    "date",
    "group_key",
    "industry_code",
    "size_bucket",
    "valid_control_count",
    "group_count",
    "quota_count",
    "selected_count",
    "eligible_share",
    "selected_share",
    "share_difference",
    "quota_error",
)


@dataclass(frozen=True)
class ConstrainedSelectionDiagnosticReport:
    """Comparison tables for baseline and proportional quota targets."""

    summary: pd.DataFrame
    coverage: pd.DataFrame
    target_overlap: pd.DataFrame
    exposure: pd.DataFrame


def calculate_constrained_selection_diagnostics(
    candidates: pd.DataFrame,
    baseline_targets: pd.DataFrame,
    *,
    holding_count: int,
    quantile_count: int = 5,
) -> ConstrainedSelectionDiagnosticReport:
    """Compare baseline targets with proportional quota targets."""

    _validate_positive_integer(holding_count, "holding_count")
    _validate_positive_integer(quantile_count, "quantile_count")
    normalized_candidates = _normalize_candidates(candidates)
    normalized_baseline = _normalize_targets(baseline_targets)
    if normalized_candidates.empty:
        raise ValueError("配额选股诊断没有可选股票池")
    _validate_target_membership(normalized_candidates, normalized_baseline)
    enriched_candidates = _attach_size_buckets(
        normalized_candidates,
        quantile_count,
    )

    candidate_counts = enriched_candidates.groupby("date", sort=True).size()
    coverage_rows = _build_baseline_coverage(
        candidate_counts,
        normalized_baseline,
    )
    overlap_rows = _build_overlap_rows(
        "baseline",
        normalized_baseline,
        normalized_baseline,
    )
    summary_rows = [
        _build_summary_row(
            "baseline",
            coverage_rows,
            overlap_rows,
            share_differences=pd.Series(dtype=float),
            quota_errors=pd.Series(dtype=float),
        )
    ]
    exposure_frames = []

    for mode in CONSTRAINED_SELECTION_MODES:
        targets, mode_coverage, mode_exposure = _build_constrained_targets(
            enriched_candidates,
            mode,
            holding_count,
            quantile_count,
        )
        coverage_rows.extend(mode_coverage)
        overlap = _build_overlap_rows(mode, normalized_baseline, targets)
        overlap_rows.extend(overlap)
        exposure_frames.append(mode_exposure)
        summary_rows.append(
            _build_summary_row(
                mode,
                mode_coverage,
                overlap,
                share_differences=mode_exposure["share_difference"],
                quota_errors=mode_exposure["quota_error"],
            )
        )

    summary = pd.DataFrame(summary_rows, columns=CONSTRAINT_SUMMARY_COLUMNS)
    coverage = pd.DataFrame(coverage_rows, columns=CONSTRAINT_COVERAGE_COLUMNS)
    target_overlap = pd.DataFrame(
        overlap_rows,
        columns=CONSTRAINT_OVERLAP_COLUMNS,
    )
    exposure = pd.concat(exposure_frames, ignore_index=True)
    return ConstrainedSelectionDiagnosticReport(
        summary,
        coverage,
        target_overlap,
        exposure,
    )


def write_constrained_selection_diagnostic_report(
    report: ConstrainedSelectionDiagnosticReport,
    output_dir: str | Path,
    *,
    parameters: dict,
) -> Path:
    """Write proportional quota diagnostic artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    (output_path / "parameters.json").write_text(
        json.dumps(parameters, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    report.summary.to_csv(
        output_path / "constraint_summary.csv",
        index=False,
        columns=CONSTRAINT_SUMMARY_COLUMNS,
    )
    report.coverage.to_csv(
        output_path / "constraint_coverage.csv",
        index=False,
        columns=CONSTRAINT_COVERAGE_COLUMNS,
    )
    report.target_overlap.to_csv(
        output_path / "constraint_target_overlap.csv",
        index=False,
        columns=CONSTRAINT_OVERLAP_COLUMNS,
    )
    report.exposure.to_csv(
        output_path / "constraint_exposure.csv",
        index=False,
        columns=CONSTRAINT_EXPOSURE_COLUMNS,
    )
    (output_path / "summary.md").write_text(
        _build_summary(report, parameters),
        encoding="utf-8",
    )
    return output_path


def _build_constrained_targets(
    candidates: pd.DataFrame,
    mode: str,
    holding_count: int,
    quantile_count: int,
) -> tuple[pd.DataFrame, list[dict], pd.DataFrame]:
    _validate_mode(mode)
    if "size_bucket" not in candidates.columns:
        candidates = _attach_size_buckets(candidates, quantile_count)
    target_rows = []
    coverage_rows = []
    exposure_rows = []
    for signal_date, group in candidates.groupby("date", sort=True):
        valid = group.loc[_mode_control_mask(group, mode)].copy()
        candidate_count = len(group)
        valid_count = len(valid)
        coverage = valid_count / candidate_count if candidate_count else None
        coverage_row = {
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
            coverage_row["status"] = "failed"
            coverage_row["failure_reason"] = (
                f"有效控制变量候选 {valid_count} 少于持仓数 {holding_count}"
            )
            coverage_rows.append(coverage_row)
            continue
        group_columns = _group_columns(mode)
        valid["group_key"] = _make_group_keys(valid, group_columns)
        group_counts = valid.groupby("group_key", sort=True).size()
        quotas = allocate_proportional_quotas(group_counts, holding_count)
        selected_frames = []
        for group_key, quota_count in quotas.items():
            group_candidates = valid[valid["group_key"] == group_key].sort_values(
                ["score", "symbol"],
                ascending=[False, True],
                kind="mergesort",
            )
            selected_frames.append(group_candidates.head(quota_count))
        selected = pd.concat(selected_frames, ignore_index=False)
        selected = selected.sort_values(
            ["score", "symbol"],
            ascending=[False, True],
            kind="mergesort",
        )
        selected["rank"] = range(1, len(selected) + 1)
        selected["target_weight"] = 1 / len(selected)
        target_rows.extend(
            selected.loc[
                :, ["date", "symbol", "score", "rank", "target_weight"]
            ].to_dict("records")
        )
        coverage_row["selected_count"] = len(selected)
        coverage_rows.append(coverage_row)
        exposure_rows.extend(
            _build_exposure_rows(
                valid,
                selected,
                quotas,
                mode,
                signal_date,
            )
        )

    targets = pd.DataFrame(
        target_rows,
        columns=["date", "symbol", "score", "rank", "target_weight"],
    )
    if not targets.empty:
        targets["date"] = pd.to_datetime(targets["date"])
    exposure = pd.DataFrame(exposure_rows, columns=CONSTRAINT_EXPOSURE_COLUMNS)
    return targets, coverage_rows, exposure


def allocate_proportional_quotas(
    group_counts: pd.Series,
    holding_count: int,
) -> dict[str, int]:
    """Allocate holdings by group size with deterministic Hamilton rounding."""

    _validate_positive_integer(holding_count, "holding_count")
    counts = pd.to_numeric(group_counts, errors="coerce")
    if counts.empty:
        raise ValueError("配额分配至少需要一个候选分组")
    if counts.isna().any() or (counts <= 0).any() or not (counts % 1 == 0).all():
        raise ValueError("候选分组数量必须是正整数")
    counts = counts.astype(int)
    if int(counts.sum()) < holding_count:
        raise ValueError("候选分组总数不能少于持仓数")

    raw = counts / counts.sum() * holding_count
    quotas = np.floor(raw).astype(int)
    remaining = holding_count - int(quotas.sum())
    remainders = raw - quotas
    order = sorted(
        counts.index.astype(str),
        key=lambda key: (-float(remainders.loc[key]), key),
    )
    while remaining:
        progressed = False
        for group_key in order:
            if remaining == 0:
                break
            if quotas.loc[group_key] >= counts.loc[group_key]:
                continue
            quotas.loc[group_key] += 1
            remaining -= 1
            progressed = True
        if not progressed:
            raise ValueError("配额分配无法满足持仓数")
    return {str(group_key): int(quotas.loc[group_key]) for group_key in order}


def _attach_size_buckets(
    candidates: pd.DataFrame,
    quantile_count: int,
) -> pd.DataFrame:
    enriched = candidates.copy()
    enriched["size_bucket"] = pd.Series(pd.NA, index=enriched.index, dtype="Int64")
    valid_size = _valid_size_mask(enriched)
    for _, group in enriched.loc[valid_size].groupby("date", sort=True):
        group = group.sort_values(
            ["market_cap", "symbol"],
            ascending=[True, True],
            kind="mergesort",
        )
        ranks = group["market_cap"].rank(
            method="first",
            pct=True,
            ascending=True,
        )
        buckets = np.ceil(ranks * quantile_count).clip(1, quantile_count).astype(int)
        enriched.loc[group.index, "size_bucket"] = buckets
    return enriched


def _build_exposure_rows(
    valid: pd.DataFrame,
    selected: pd.DataFrame,
    quotas: dict[str, int],
    mode: str,
    signal_date: pd.Timestamp,
) -> list[dict]:
    selected_counts = selected.groupby("group_key", sort=True).size()
    valid_count = len(valid)
    rows = []
    group_columns = _group_columns(mode)
    for group_key, group in valid.groupby("group_key", sort=True):
        group_count = len(group)
        selected_count = int(selected_counts.get(group_key, 0))
        quota_count = quotas[group_key]
        selected_share = selected_count / len(selected)
        eligible_share = group_count / valid_count
        row = {
            "mode": mode,
            "date": signal_date.date().isoformat(),
            "group_key": group_key,
            "industry_code": None,
            "size_bucket": None,
            "valid_control_count": valid_count,
            "group_count": group_count,
            "quota_count": quota_count,
            "selected_count": selected_count,
            "eligible_share": eligible_share,
            "selected_share": selected_share,
            "share_difference": selected_share - eligible_share,
            "quota_error": selected_count - quota_count,
        }
        if "industry_code" in group_columns:
            row["industry_code"] = group["industry_code"].iloc[0]
        if "size_bucket" in group_columns:
            row["size_bucket"] = int(group["size_bucket"].iloc[0])
        rows.append(row)
    return rows


def _mode_control_mask(group: pd.DataFrame, mode: str) -> pd.Series:
    if mode == "industry_quota":
        return group["industry_code"].notna()
    if mode == "size_quota":
        return _valid_size_mask(group)
    return group["industry_code"].notna() & _valid_size_mask(group)


def _valid_size_mask(group: pd.DataFrame) -> pd.Series:
    return (
        group["market_cap"].notna()
        & np.isfinite(group["market_cap"])
        & group["market_cap"].gt(0)
    )


def _group_columns(mode: str) -> tuple[str, ...]:
    if mode == "industry_quota":
        return ("industry_code",)
    if mode == "size_quota":
        return ("size_bucket",)
    return ("industry_code", "size_bucket")


def _make_group_keys(
    frame: pd.DataFrame,
    group_columns: tuple[str, ...],
) -> pd.Series:
    if len(group_columns) == 1:
        return frame[group_columns[0]].astype("string")
    return frame.loc[:, list(group_columns)].astype("string").agg("|".join, axis=1)


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
                "constrained_target_count": len(current_set),
                "overlap_count": len(baseline_set & current_set),
                "overlap_rate": (
                    len(baseline_set & current_set) / len(baseline_set)
                    if baseline_set and current_set
                    else None
                ),
            }
        )
    return rows


def _build_summary_row(
    mode: str,
    coverage_rows: list[dict],
    overlap_rows: list[dict],
    *,
    share_differences: pd.Series,
    quota_errors: pd.Series,
) -> dict:
    coverage = pd.DataFrame(coverage_rows)
    overlap = pd.DataFrame(overlap_rows)
    return {
        "mode": mode,
        "signal_date_count": len(coverage),
        "successful_signal_date_count": int((coverage["status"] == "success").sum()),
        "failed_signal_date_count": int((coverage["status"] == "failed").sum()),
        "mean_control_coverage_rate": coverage["control_coverage_rate"].mean(),
        "min_control_coverage_rate": coverage["control_coverage_rate"].min(),
        "mean_target_overlap_rate": overlap["overlap_rate"].mean(),
        "mean_abs_share_difference": (
            float(share_differences.abs().mean())
            if not share_differences.empty
            else None
        ),
        "max_abs_share_difference": (
            float(share_differences.abs().max())
            if not share_differences.empty
            else None
        ),
        "max_abs_quota_error": (
            float(quota_errors.abs().max()) if not quota_errors.empty else None
        ),
    }


def _target_sets(targets: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
    return {
        signal_date: set(group["symbol"])
        for signal_date, group in targets.groupby("date", sort=True)
    }


def _validate_mode(mode: str) -> None:
    if mode not in CONSTRAINED_SELECTION_MODES:
        raise ValueError(
            "不支持的配额选股模式: "
            + str(mode)
            + "；可选: "
            + ", ".join(CONSTRAINED_SELECTION_MODES)
        )


def _validate_positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} 必须是正整数")


def _build_summary(
    report: ConstrainedSelectionDiagnosticReport,
    parameters: dict,
) -> str:
    lines = [
        "# 因子实验行业/规模配额选股对照诊断",
        "",
        "本报告固定原始综合评分，仅按有效行业、规模或行业×规模候选池比例分配持仓配额；它是研究对照，不改变正式策略或收益结果。",
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
            ("配额模式", ", ".join(CONSTRAINED_SELECTION_MODES)),
            ("配额方法", "Hamilton 最大余数法；组内按原始 score 降序"),
            ("规模分组", f"{parameters.get('quantile_count', '—')} 组"),
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
            "平均占比差绝对值",
            "最大配额误差",
        ],
        [
            (
                row["mode"],
                int(row["successful_signal_date_count"]),
                int(row["failed_signal_date_count"]),
                _format_percent(row["mean_control_coverage_rate"]),
                _format_percent(row["mean_target_overlap_rate"]),
                _format_percent(row["mean_abs_share_difference"]),
                _format_number(row["max_abs_quota_error"]),
            )
            for _, row in report.summary.iterrows()
        ],
    )
    lines.extend(
        [
            "",
            "## 3. 解释边界",
            "",
            "配额按各模式有效候选池计算，缺失控制变量的候选只进入完整候选池覆盖审计，不参与对应配额。有效候选不足持仓数的信号日不会回退。配额误差为实际入选数减目标配额，目标重合率和低占比差不等于样本外收益改善；联合行业×规模配额可能因持仓数有限而显著改变原始目标。",
            "",
            "## 4. 审计文件",
            "",
        ]
    )
    _append_table(
        lines,
        ["文件", "内容"],
        [
            ("summary.md", "人读摘要和解释边界"),
            ("parameters.json", "配置、数据源和快照元数据"),
            ("constraint_summary.csv", "各模式信号日、覆盖率、重合率和配额误差"),
            ("constraint_coverage.csv", "逐模式、逐信号日有效候选覆盖和失败原因"),
            ("constraint_target_overlap.csv", "逐模式、逐信号日与基准目标重合"),
            ("constraint_exposure.csv", "逐模式、逐组配额、入选数和占比差"),
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


def _format_number(value) -> str:
    return "—" if pd.isna(value) else f"{float(value):.0f}"
