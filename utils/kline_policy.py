"""Shared policies for stock K-line ingestion."""

from datetime import datetime

import pandas as pd

from config.settings import MIN_KLINE_START_DATE

_MIN_KLINE_START_DATE = datetime.strptime(MIN_KLINE_START_DATE, "%Y%m%d").date()
KNOWN_BAD_KLINE_ROWS = frozenset(
    {
        ("688089", "2024-11-06"),
        ("688143", "2024-11-06"),
        ("688173", "2024-11-06"),
    }
)


def normalize_kline_start_date(start_date: str | None) -> str:
    """Clamp a requested K-line start date to the configured minimum date."""
    if start_date is None:
        return MIN_KLINE_START_DATE
    try:
        parsed = datetime.strptime(start_date, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"start_date 必须是 YYYYMMDD: {start_date!r}") from exc
    return max(parsed, _MIN_KLINE_START_DATE).strftime("%Y%m%d")


def drop_known_bad_kline_rows(
    frame: pd.DataFrame, symbol: str
) -> tuple[pd.DataFrame, set[str]]:
    """Drop explicitly identified source rows and return their dates."""
    previous_count = int(frame.attrs.get("known_bad_rows_filtered", 0) or 0)
    if frame.empty:
        filtered = frame.copy()
        filtered.attrs["known_bad_rows_filtered"] = previous_count
        return filtered, set()

    bad_dates = {
        row_date
        for row_symbol, row_date in KNOWN_BAD_KLINE_ROWS
        if row_symbol == symbol
    }
    if not bad_dates:
        filtered = frame.copy()
        filtered.attrs["known_bad_rows_filtered"] = previous_count
        return filtered, set()

    normalized_dates = pd.to_datetime(frame["date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    excluded_dates = set(normalized_dates[normalized_dates.isin(bad_dates)].dropna())
    filtered = frame.loc[~normalized_dates.isin(excluded_dates)].reset_index(drop=True)
    filtered.attrs = frame.attrs.copy()
    filtered.attrs["known_bad_rows_filtered"] = previous_count + len(excluded_dates)
    return filtered, excluded_dates
