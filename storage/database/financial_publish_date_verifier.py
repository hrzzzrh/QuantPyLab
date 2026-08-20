"""对超出法定期限的财报公告日期进行交易所官方二次核验。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from config.settings import WAREHOUSE_DIR
from storage.database.financial_publish_date_reconciler import (
    DATA_AVAILABLE_DATE_COLUMN,
    FINANCIAL_SOURCE_CATEGORIES,
    PUBLISH_DATE_COLUMN,
    REPORT_DATE_COLUMN,
    build_data_available_date_map,
    normalize_financial_dates,
)
from storage.database.official_disclosure_date_resolver import (
    OfficialDisclosure,
    OfficialDisclosureDateResolver,
    OfficialDisclosureQueryError,
    get_disclosure_deadline,
    is_publish_date_overdue,
    normalize_report_date,
    parse_date_value,
)
from storage.file_store.atomic_partition_store import save_partitions_atomically
from utils.logger import logger


@dataclass(frozen=True)
class OverduePublishDateVerification:
    """单只股票公告日期二次核验结果。"""

    overdue_report_dates: tuple[str, ...] = ()
    official_dates: Mapping[str, OfficialDisclosure] = field(default_factory=dict)
    unresolved_report_dates: tuple[str, ...] = ()
    changed_rows: Mapping[str, int] = field(default_factory=dict)


def _source_path(base_dir: Path, source_name: str, symbol: str) -> Path:
    return (
        base_dir
        / FINANCIAL_SOURCE_CATEGORIES[source_name]
        / f"symbol={symbol}"
        / "data.parquet"
    )


def load_financial_source_frames(
    symbol: str,
    warehouse_dir: str | Path | None = None,
) -> dict[str, pd.DataFrame]:
    """读取指定股票已有的四类财务来源数据。"""
    base_dir = Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    frames: dict[str, pd.DataFrame] = {}
    for source_name in FINANCIAL_SOURCE_CATEGORIES:
        path = _source_path(base_dir, source_name, symbol)
        if path.exists():
            frames[source_name] = pd.read_parquet(path)
    return frames


def build_current_publish_date_map(
    source_frames: Mapping[str, pd.DataFrame],
) -> dict[str, str]:
    """按当前四源口径生成报告期到公告日期的映射。"""
    date_frames: list[pd.DataFrame] = []
    for frame in source_frames.values():
        if REPORT_DATE_COLUMN not in frame or PUBLISH_DATE_COLUMN not in frame:
            continue
        dates = frame[[REPORT_DATE_COLUMN, PUBLISH_DATE_COLUMN]].copy()
        dates[REPORT_DATE_COLUMN] = normalize_financial_dates(dates[REPORT_DATE_COLUMN])
        dates[PUBLISH_DATE_COLUMN] = normalize_financial_dates(
            dates[PUBLISH_DATE_COLUMN]
        )
        date_frames.append(dates.dropna())

    if not date_frames:
        return {}

    combined = pd.concat(date_frames, ignore_index=True)
    if combined.empty:
        return {}
    canonical_dates = (
        combined.groupby(REPORT_DATE_COLUMN)[PUBLISH_DATE_COLUMN].min().astype("string")
    )
    return {
        str(report_date): str(publish_date)
        for report_date, publish_date in canonical_dates.items()
    }


def find_overdue_publish_dates(
    source_frames: Mapping[str, pd.DataFrame],
    today: date,
) -> dict[str, str]:
    """找出当前四源口径下已经晚于法定截止日的报告期。"""
    current_dates = build_current_publish_date_map(source_frames)
    return {
        report_date: publish_date
        for report_date, publish_date in current_dates.items()
        if get_disclosure_deadline(report_date) is not None
        and is_publish_date_overdue(report_date, publish_date)
        and parse_date_value(publish_date) is not None
        and parse_date_value(publish_date) <= today
    }


def apply_official_publish_dates(
    symbol: str,
    official_dates: Mapping[str, OfficialDisclosure],
    source_frames: Mapping[str, pd.DataFrame],
    warehouse_dir: str | Path | None = None,
) -> dict[str, int]:
    """将官方首发日期覆盖到已有四类来源的公告日期字段。"""
    if not official_dates:
        return {}

    base_dir = Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    official_date_map = {
        normalize_report_date(report_date): disclosure.publish_date.strftime("%Y%m%d")
        for report_date, disclosure in official_dates.items()
        if normalize_report_date(report_date) is not None
    }
    if not official_date_map:
        return {}

    available_dates = build_data_available_date_map(source_frames).to_dict()
    for report_date, publish_date in official_date_map.items():
        existing_date = available_dates.get(report_date)
        available_dates[report_date] = (
            max(str(existing_date or ""), publish_date) or publish_date
        )

    changed_rows: dict[str, int] = {}
    pending_writes: list[tuple[pd.DataFrame, str, str]] = []
    for source_name, frame in source_frames.items():
        if (
            REPORT_DATE_COLUMN not in frame
            or PUBLISH_DATE_COLUMN not in frame
            or source_name not in FINANCIAL_SOURCE_CATEGORIES
        ):
            continue

        report_dates = normalize_financial_dates(frame[REPORT_DATE_COLUMN])
        official_for_rows = report_dates.map(official_date_map)
        available_for_rows = report_dates.map(available_dates)
        has_official_date = official_for_rows.notna()
        has_available_date = available_for_rows.notna()
        if not has_official_date.any() and not has_available_date.any():
            continue

        current_dates = normalize_financial_dates(frame[PUBLISH_DATE_COLUMN])
        current_available_dates = (
            normalize_financial_dates(frame[DATA_AVAILABLE_DATE_COLUMN])
            if DATA_AVAILABLE_DATE_COLUMN in frame
            else pd.Series(pd.NA, index=frame.index, dtype="string")
        )
        changed_publish = has_official_date & (
            current_dates.fillna("") != official_for_rows.fillna("")
        )
        changed_available = has_available_date & (
            current_available_dates.fillna("") != available_for_rows.fillna("")
        )
        changed_count = int((changed_publish | changed_available).sum())
        if changed_count == 0:
            continue

        # 官方核验只覆盖公告日期；原始字段及其数值不保留为额外副本。
        frame_to_save = frame.copy()
        frame_to_save.loc[has_official_date, PUBLISH_DATE_COLUMN] = official_for_rows[
            has_official_date
        ].astype(str)
        frame_to_save.loc[has_available_date, DATA_AVAILABLE_DATE_COLUMN] = (
            available_for_rows[has_available_date].astype(str)
        )
        pending_writes.append(
            (frame_to_save, FINANCIAL_SOURCE_CATEGORIES[source_name], symbol)
        )
        changed_rows[source_name] = changed_count

    save_partitions_atomically(base_dir, pending_writes)

    return changed_rows


def _log_official_dates(
    symbol: str,
    official_dates: Mapping[str, OfficialDisclosure],
) -> None:
    for report_date, disclosure in official_dates.items():
        deadline = get_disclosure_deadline(report_date)
        if deadline is not None and disclosure.publish_date > deadline:
            logger.warning(
                "%s %s 官方首发公告日期仍晚于法定截止日: %s > %s (%s)",
                symbol,
                report_date,
                disclosure.publish_date,
                deadline,
                disclosure.url or disclosure.title,
            )
        else:
            logger.info(
                "%s %s 官方公告日期核验通过: %s (%s)",
                symbol,
                report_date,
                disclosure.publish_date,
                disclosure.url or disclosure.title,
            )


def verify_overdue_financial_publish_dates_for_symbol(
    symbol: str,
    resolver: OfficialDisclosureDateResolver | None = None,
    warehouse_dir: str | Path | None = None,
    today: date | None = None,
    minimum_report_date: str | date | None = None,
) -> OverduePublishDateVerification:
    """仅对指定范围内的超期报告查询官方公告，并回填首发日期。"""
    current_date = today or date.today()
    source_frames = load_financial_source_frames(symbol, warehouse_dir)
    overdue_dates = find_overdue_publish_dates(source_frames, current_date)
    if minimum_report_date is not None:
        minimum_date = parse_date_value(minimum_report_date)
        if minimum_date is None:
            raise ValueError(f"minimum_report_date 无法解析: {minimum_report_date}")
        overdue_dates = {
            report_date: publish_date
            for report_date, publish_date in overdue_dates.items()
            if (report_day := parse_date_value(report_date)) is not None
            and report_day >= minimum_date
        }
    if not overdue_dates:
        return OverduePublishDateVerification()

    official_resolver = resolver or OfficialDisclosureDateResolver(
        today_provider=lambda: current_date
    )
    publish_dates = [
        parsed_date
        for publish_date in overdue_dates.values()
        if (parsed_date := parse_date_value(publish_date)) is not None
    ]
    end_date = max([current_date, *publish_dates])
    report_dates = tuple(sorted(overdue_dates))

    try:
        official_dates = official_resolver.resolve_overdue_report_dates(
            symbol,
            report_dates,
            end_date=end_date,
        )
    except (OfficialDisclosureQueryError, requests.RequestException) as exc:
        logger.warning("%s 官方财报公告日期二次核验失败，保留现有日期: %s", symbol, exc)
        for report_date in report_dates:
            logger.warning("%s %s 未完成官方公告日期核验", symbol, report_date)
        return OverduePublishDateVerification(
            overdue_report_dates=report_dates,
            unresolved_report_dates=report_dates,
        )

    _log_official_dates(symbol, official_dates)
    unresolved_dates = tuple(
        report_date for report_date in report_dates if report_date not in official_dates
    )
    for report_date in unresolved_dates:
        logger.warning("%s %s 未找到匹配的官方原始定期报告公告", symbol, report_date)

    changed_rows = apply_official_publish_dates(
        symbol,
        official_dates,
        source_frames,
        warehouse_dir,
    )
    if changed_rows:
        logger.info(
            "%s 官方公告日期回填完成: %s",
            symbol,
            ", ".join(f"{name}={count}" for name, count in changed_rows.items()),
        )
    return OverduePublishDateVerification(
        overdue_report_dates=report_dates,
        official_dates=official_dates,
        unresolved_report_dates=unresolved_dates,
        changed_rows=changed_rows,
    )
