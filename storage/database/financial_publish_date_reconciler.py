"""统一财务报告期的公告日期口径。"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from config.settings import WAREHOUSE_DIR
from storage.file_store.atomic_partition_store import save_partitions_atomically
from utils.logger import logger

PUBLISH_DATE_COLUMN = "公告日期"
DATA_AVAILABLE_DATE_COLUMN = "数据可用日期"
REPORT_DATE_COLUMN = "report_date"

FINANCIAL_SOURCE_CATEGORIES = {
    "balance": "financial_statements/type=balance",
    "income": "financial_statements/type=income",
    "cashflow": "financial_statements/type=cashflow",
    "indicator": "indicators",
}


def normalize_financial_dates(series: pd.Series) -> pd.Series:
    """将报告期或公告日期标准化为 YYYYMMDD 字符串。"""
    text = series.astype("string").str.strip()
    digits = text.str.replace(r"\D", "", regex=True)
    normalized = digits.where(digits.str.len() >= 8).str[:8]
    valid_dates = pd.to_datetime(normalized, format="%Y%m%d", errors="coerce")
    return normalized.where(valid_dates.notna())


def _max_date_series(left: pd.Series, right: pd.Series) -> pd.Series:
    """按 YYYYMMDD 字符串取两个日期序列的较晚非空值。"""
    result = left.copy()
    right_available = right.notna()
    result = result.where(result.notna(), right)
    both_available = result.notna() & right_available
    result.loc[both_available & (right > result)] = right.loc[
        both_available & (right > result)
    ]
    return result.astype("string")


def build_canonical_publish_date_map(
    source_frames: Mapping[str, pd.DataFrame],
) -> pd.Series:
    """按报告期计算四个来源的最早非空公告日期。"""
    date_frames = []
    for frame in source_frames.values():
        if REPORT_DATE_COLUMN not in frame or PUBLISH_DATE_COLUMN not in frame:
            continue

        dates = frame[[REPORT_DATE_COLUMN, PUBLISH_DATE_COLUMN]].copy()
        dates[REPORT_DATE_COLUMN] = normalize_financial_dates(dates[REPORT_DATE_COLUMN])
        dates[PUBLISH_DATE_COLUMN] = normalize_financial_dates(
            dates[PUBLISH_DATE_COLUMN]
        )
        date_frames.append(dates)

    if not date_frames:
        return pd.Series(dtype="string", name=PUBLISH_DATE_COLUMN)

    combined_dates = pd.concat(date_frames, ignore_index=True)
    combined_dates = combined_dates.dropna(
        subset=[REPORT_DATE_COLUMN, PUBLISH_DATE_COLUMN]
    )
    if combined_dates.empty:
        return pd.Series(dtype="string", name=PUBLISH_DATE_COLUMN)

    return (
        combined_dates.groupby(REPORT_DATE_COLUMN)[PUBLISH_DATE_COLUMN]
        .min()
        .astype("string")
    )


def build_data_available_date_map(
    source_frames: Mapping[str, pd.DataFrame],
) -> pd.Series:
    """按报告期计算四个来源全部可用后的安全生效日期。

    新字段是由现有日期推导出的 ASOF 安全边界，不是任一来源的原始日期副本。
    对尚未生成该字段的旧分区，先用当前 ``公告日期`` 作为回退值。
    """
    date_frames = []
    for frame in source_frames.values():
        if REPORT_DATE_COLUMN not in frame or PUBLISH_DATE_COLUMN not in frame:
            continue

        dates = frame[[REPORT_DATE_COLUMN, PUBLISH_DATE_COLUMN]].copy()
        dates[REPORT_DATE_COLUMN] = normalize_financial_dates(dates[REPORT_DATE_COLUMN])
        publish_dates = normalize_financial_dates(dates[PUBLISH_DATE_COLUMN])
        if DATA_AVAILABLE_DATE_COLUMN in frame:
            available_dates = normalize_financial_dates(
                frame[DATA_AVAILABLE_DATE_COLUMN]
            )
            dates[DATA_AVAILABLE_DATE_COLUMN] = _max_date_series(
                available_dates, publish_dates
            )
        else:
            dates[DATA_AVAILABLE_DATE_COLUMN] = publish_dates
        date_frames.append(dates)

    if not date_frames:
        return pd.Series(dtype="string", name=DATA_AVAILABLE_DATE_COLUMN)

    combined_dates = pd.concat(date_frames, ignore_index=True).dropna(
        subset=[REPORT_DATE_COLUMN, DATA_AVAILABLE_DATE_COLUMN]
    )
    if combined_dates.empty:
        return pd.Series(dtype="string", name=DATA_AVAILABLE_DATE_COLUMN)

    return (
        combined_dates.groupby(REPORT_DATE_COLUMN)[DATA_AVAILABLE_DATE_COLUMN]
        .max()
        .astype("string")
    )


def reconcile_financial_publish_dates_for_symbol(
    symbol: str,
    warehouse_dir: str | Path | None = None,
) -> dict[str, int]:
    """覆盖指定股票四类财务数据的公告日期。"""
    base_dir = Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    source_frames: dict[str, pd.DataFrame] = {}
    source_paths: dict[str, Path] = {}

    for source_name, category in FINANCIAL_SOURCE_CATEGORIES.items():
        path = base_dir / category / f"symbol={symbol}" / "data.parquet"
        if not path.exists():
            continue
        source_frames[source_name] = pd.read_parquet(path)
        source_paths[source_name] = path

    canonical_dates = build_canonical_publish_date_map(source_frames)
    available_dates = build_data_available_date_map(source_frames)
    if canonical_dates.empty and available_dates.empty:
        return {}

    changed_rows: dict[str, int] = {}

    pending_writes: list[tuple[pd.DataFrame, str, str]] = []
    for source_name, frame in source_frames.items():
        if (
            REPORT_DATE_COLUMN not in frame
            or PUBLISH_DATE_COLUMN not in frame
            or source_name not in source_paths
        ):
            continue

        report_dates = normalize_financial_dates(frame[REPORT_DATE_COLUMN])
        canonical_for_rows = report_dates.map(canonical_dates)
        available_for_rows = report_dates.map(available_dates)
        has_canonical_date = canonical_for_rows.notna()
        has_available_date = available_for_rows.notna()
        if not has_canonical_date.any() and not has_available_date.any():
            continue

        current_dates = normalize_financial_dates(frame[PUBLISH_DATE_COLUMN])
        current_available_dates = (
            normalize_financial_dates(frame[DATA_AVAILABLE_DATE_COLUMN])
            if DATA_AVAILABLE_DATE_COLUMN in frame
            else pd.Series(pd.NA, index=frame.index, dtype="string")
        )
        changed_publish = has_canonical_date & (
            current_dates.fillna("") != canonical_for_rows.fillna("")
        )
        changed_available = has_available_date & (
            current_available_dates.fillna("") != available_for_rows.fillna("")
        )
        changed = changed_publish | changed_available
        changed_count = int(changed.sum())
        if changed_count == 0:
            continue

        # 同一报告期可能在后续公告中再次出现。这里统一取四源最早非空日期，
        # 作为“首次披露日期”口径并覆盖四张表；不保留来源间的原始日期差异。
        # 数据可用日期是四源日期的最大值，只用于 TTM/ASOF 的安全生效边界，
        # 不代表任一来源的原始字段。
        frame_to_save = frame.copy()
        frame_to_save.loc[has_canonical_date, PUBLISH_DATE_COLUMN] = canonical_for_rows[
            has_canonical_date
        ].astype(str)
        frame_to_save.loc[has_available_date, DATA_AVAILABLE_DATE_COLUMN] = (
            available_for_rows[has_available_date].astype(str)
        )
        pending_writes.append(
            (frame_to_save, FINANCIAL_SOURCE_CATEGORIES[source_name], symbol)
        )
        changed_rows[source_name] = changed_count

    save_partitions_atomically(base_dir, pending_writes)

    if changed_rows:
        logger.info(
            "已统一 %s 的财务公告日期: %s",
            symbol,
            ", ".join(f"{name}={count}" for name, count in changed_rows.items()),
        )
    return changed_rows
