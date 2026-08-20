"""Shared K-line frame validation for ingestion and migration paths."""

from __future__ import annotations

from math import isfinite

import pandas as pd

REQUIRED_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adj_factor",
)
STORED_COLUMNS = REQUIRED_COLUMNS


class KlineValidationError(ValueError):
    """Raised when a K-line frame violates structural validation."""


class KlineQualityError(KlineValidationError):
    """Raised when a K-line frame violates an auditable quality gate."""


def validate_frame(
    df: pd.DataFrame, *, require_close_hfq: bool = False
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise KlineValidationError("K 线数据为空")

    required_columns = (
        REQUIRED_COLUMNS + ("close_hfq",) if require_close_hfq else REQUIRED_COLUMNS
    )
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise KlineValidationError(f"K 线数据缺少字段: {', '.join(missing)}")

    normalized = df.copy()
    try:
        parsed_dates = pd.to_datetime(normalized["date"], errors="coerce")
    except Exception as exc:
        raise KlineValidationError("K 线日期无法解析") from exc
    if parsed_dates.isna().any():
        raise KlineValidationError("K 线日期包含空值或无效日期")
    normalized["date"] = parsed_dates.dt.date

    if normalized["date"].duplicated().any():
        raise KlineValidationError("K 线存在重复交易日期")
    if not normalized["date"].is_monotonic_increasing:
        raise KlineValidationError("K 线日期未按升序排列")

    numeric_columns = [column for column in REQUIRED_COLUMNS if column != "date"]
    for column in numeric_columns:
        try:
            normalized[column] = pd.to_numeric(
                normalized[column], errors="raise"
            ).astype(float)
        except Exception as exc:
            raise KlineValidationError(f"字段 {column} 含非数值内容") from exc
        if not normalized[column].map(isfinite).all():
            raise KlineValidationError(f"字段 {column} 含非有限数值")

    for column in ("open", "high", "low", "close", "volume", "amount"):
        if (normalized[column] < 0).any():
            raise KlineValidationError(f"字段 {column} 含负数")
    if (normalized["adj_factor"] <= 0).any():
        raise KlineValidationError("复权因子必须大于 0")
    if (normalized["high"] < normalized["low"]).any():
        raise KlineValidationError("最高价不能低于最低价")
    if (normalized["open"] > normalized["high"]).any() or (
        normalized["open"] < normalized["low"]
    ).any():
        raise KlineValidationError("开盘价必须位于当日最高价和最低价之间")
    if (normalized["close"] > normalized["high"]).any() or (
        normalized["close"] < normalized["low"]
    ).any():
        raise KlineValidationError("收盘价必须位于当日最高价和最低价之间")

    if "close_hfq" in normalized.columns:
        try:
            normalized["close_hfq"] = pd.to_numeric(
                normalized["close_hfq"], errors="raise"
            ).astype(float)
        except Exception as exc:
            raise KlineValidationError("close_hfq 含非数值内容") from exc
        if not normalized["close_hfq"].map(isfinite).all():
            raise KlineValidationError("close_hfq 含非有限数值")
        if (normalized["close_hfq"] < 0).any():
            raise KlineValidationError("close_hfq 含负数")

    return normalized.sort_values("date").reset_index(drop=True)


def quality_metrics(df: pd.DataFrame) -> dict[str, int | float]:
    dates = pd.to_datetime(df["date"])
    if "close_hfq" in df.columns:
        relation_error = (df["close"] * df["adj_factor"] - df["close_hfq"]).abs()
        relation_tolerance = df["close_hfq"].abs() * 1e-9 + 1e-8
        mismatch = relation_error > relation_tolerance
        relation_max_abs_error = float(relation_error.max())
        relation_mismatch_count = int(mismatch.sum())
    else:
        relation_max_abs_error = 0.0
        relation_mismatch_count = 0
    return {
        "new_rows": len(df),
        "weekend_rows": int((dates.dt.dayofweek >= 5).sum()),
        "weekend_rows_filtered": int(df.attrs.get("weekend_rows_filtered", 0) or 0),
        "known_bad_rows_filtered": int(df.attrs.get("known_bad_rows_filtered", 0) or 0),
        "zero_volume_rows": int((df["volume"] == 0).sum()),
        "zero_amount_rows": int((df["amount"] == 0).sum()),
        "factor_one_rows": int((df["adj_factor"] == 1.0).sum()),
        "factor_min": float(df["adj_factor"].min()),
        "factor_max": float(df["adj_factor"].max()),
        "hfq_source_rows": int(df.attrs.get("hfq_source_rows", 0)),
        "hfq_forward_filled_rows": int(df.attrs.get("hfq_forward_filled_rows", 0)),
        "hfq_relation_mismatch_count": relation_mismatch_count,
        "hfq_relation_max_abs_error": relation_max_abs_error,
    }


def validate_kline_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Validate a KLC frame before it is persisted to a stock partition."""
    normalized = validate_frame(df, require_close_hfq=True)
    metrics = quality_metrics(normalized)
    if metrics["weekend_rows"]:
        raise KlineQualityError("K 线包含周末日期")
    if metrics["hfq_relation_mismatch_count"]:
        raise KlineQualityError("close_hfq 与 close * adj_factor 不一致")
    return normalized
