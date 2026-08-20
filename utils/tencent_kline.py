"""Tencent newfqkline daily K-line fetcher for delisted-stock rebuilding."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from math import isfinite

import pandas as pd
import requests

from config.settings import MIN_KLINE_START_DATE
from utils.financial import to_sina_symbol
from utils.kline_policy import drop_known_bad_kline_rows, normalize_kline_start_date
from utils.kline_validation import validate_kline_frame

TENCENT_NEW_FQKLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get"
TENCENT_PAGE_SIZE = 640
TENCENT_MAX_PAGES = 60
TENCENT_AMOUNT_SCALE = 10_000.0
TENCENT_HEADERS = {"User-Agent": "Mozilla/5.0"}
TENCENT_API_START_DATE = (
    datetime.strptime(MIN_KLINE_START_DATE, "%Y%m%d").date().isoformat()
)
TENCENT_OUTPUT_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "close_hfq",
    "adj_factor",
)


def _drop_weekend_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, set[date]]:
    if frame.empty:
        return frame.copy(), set()
    weekend_mask = pd.to_datetime(frame["date"]).dt.dayofweek >= 5
    weekend_dates = set(frame.loc[weekend_mask, "date"])
    return frame.loc[~weekend_mask].reset_index(drop=True), weekend_dates


class TencentKlineFetchError(RuntimeError):
    """Raised when Tencent K-line data cannot be fetched or parsed."""

    source_used = "tencent-newfq"


class TencentKlineTransientError(TencentKlineFetchError):
    """Raised when Tencent transport temporarily fails."""


def _parse_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} 必须是 YYYYMMDD: {value!r}") from exc


def _parse_number(value: object, field_name: str, row_number: int) -> float:
    if value is None or isinstance(value, bool) or str(value).strip() == "":
        raise TencentKlineFetchError(f"腾讯 K 线第 {row_number} 行 {field_name} 为空")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise TencentKlineFetchError(
            f"腾讯 K 线第 {row_number} 行 {field_name} 非数值"
        ) from exc
    if not isfinite(parsed):
        raise TencentKlineFetchError(
            f"腾讯 K 线第 {row_number} 行 {field_name} 非有限数值"
        )
    return parsed


def _parse_row_date(row: object, *, symbol: str, row_number: int) -> date:
    if not isinstance(row, list) or len(row) < 9:
        raise TencentKlineFetchError(f"腾讯 K 线第 {row_number} 行字段不足: {symbol}")
    try:
        return pd.to_datetime(row[0], errors="raise").date()
    except (TypeError, ValueError) as exc:
        raise TencentKlineFetchError(
            f"腾讯 K 线第 {row_number} 行日期无效: {symbol}"
        ) from exc


def _parse_rows(rows: object, *, symbol: str, adjust: str) -> pd.DataFrame:
    if not isinstance(rows, list):
        raise TencentKlineFetchError(
            f"腾讯 K 线响应缺少有效行列表: {symbol}, adjust={adjust or 'raw'}"
        )

    parsed_rows: list[dict[str, object]] = []
    for row_number, row in enumerate(rows, start=1):
        parsed_date = _parse_row_date(row, symbol=symbol, row_number=row_number)

        close = _parse_number(row[2], "close", row_number)
        amount_wan = _parse_number(row[8], "amount_wan", row_number)
        if amount_wan < 0:
            raise TencentKlineFetchError(
                f"腾讯 K 线第 {row_number} 行成交额为负数: {symbol}"
            )

        parsed_row = {
            "date": parsed_date,
            "open": _parse_number(row[1], "open", row_number),
            "high": _parse_number(row[3], "high", row_number),
            "low": _parse_number(row[4], "low", row_number),
            "volume": _parse_number(row[5], "volume", row_number),
            "amount": amount_wan * TENCENT_AMOUNT_SCALE,
        }
        parsed_row["close_hfq" if adjust == "hfq" else "close"] = close
        parsed_rows.append(parsed_row)

    frame = pd.DataFrame(parsed_rows)
    if frame.empty:
        return frame
    if frame["date"].duplicated().any():
        raise TencentKlineFetchError(f"腾讯 K 线包含重复日期: {symbol}")
    return frame.sort_values("date").reset_index(drop=True)


class TencentKlineFetcher:
    """Fetch complete raw and backward-adjusted Tencent daily K-lines."""

    @staticmethod
    def fetch_page(
        tencent_symbol: str, end_date: str, adjust: str
    ) -> list[list[object]]:
        if adjust not in {"", "hfq"}:
            raise ValueError(f"不支持的腾讯复权类型: {adjust!r}")
        try:
            response = requests.get(
                TENCENT_NEW_FQKLINE_URL,
                params={
                    "param": (
                        f"{tencent_symbol},day,{TENCENT_API_START_DATE},"
                        f"{end_date},"
                        f"{TENCENT_PAGE_SIZE},{adjust}"
                    )
                },
                headers=TENCENT_HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code in {408, 425, 429} or (
                isinstance(status_code, int) and status_code >= 500
            ):
                raise TencentKlineTransientError(
                    f"腾讯 K 线请求暂时失败: {tencent_symbol}, "
                    f"adjust={adjust or 'raw'}, status={status_code}: {exc}"
                ) from exc
            raise TencentKlineFetchError(
                f"腾讯 K 线请求失败: {tencent_symbol}, "
                f"adjust={adjust or 'raw'}, status={status_code}: {exc}"
            ) from exc
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise TencentKlineTransientError(
                f"腾讯 K 线请求暂时失败: {tencent_symbol}, "
                f"adjust={adjust or 'raw'}: {exc}"
            ) from exc
        except ValueError as exc:
            raise TencentKlineFetchError(
                f"腾讯 K 线请求响应无法解析: {tencent_symbol}, "
                f"adjust={adjust or 'raw'}: {exc}"
            ) from exc
        except requests.RequestException as exc:
            raise TencentKlineTransientError(
                f"腾讯 K 线请求暂时失败: {tencent_symbol}, "
                f"adjust={adjust or 'raw'}: {exc}"
            ) from exc
        except Exception as exc:
            raise TencentKlineFetchError(
                f"腾讯 K 线请求失败: {tencent_symbol}, adjust={adjust or 'raw'}: {exc}"
            ) from exc

        if not isinstance(payload, dict):
            raise TencentKlineFetchError(
                f"腾讯 K 线响应不是 JSON 对象: {tencent_symbol}"
            )
        if payload.get("code") != 0:
            raise TencentKlineFetchError(
                f"腾讯 K 线接口返回错误: {tencent_symbol}, "
                f"adjust={adjust or 'raw'}, code={payload.get('code')!r}, "
                f"msg={payload.get('msg')!r}"
            )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise TencentKlineFetchError(
                f"腾讯 K 线响应缺少有效 data 对象: {tencent_symbol}"
            )
        symbol_data = data.get(tencent_symbol)
        if not isinstance(symbol_data, dict):
            raise TencentKlineFetchError(f"腾讯 K 线响应缺少股票数据: {tencent_symbol}")
        key = "hfqday" if adjust == "hfq" else "day"
        rows = symbol_data.get(key)
        if rows is None:
            raise TencentKlineFetchError(
                f"腾讯 K 线响应缺少 {key} 字段: {tencent_symbol}"
            )
        if not isinstance(rows, list):
            raise TencentKlineFetchError(
                f"腾讯 K 线 {key} 字段不是列表: {tencent_symbol}"
            )
        return rows

    @staticmethod
    def fetch_series(
        tencent_symbol: str,
        *,
        adjust: str,
        start_date: str = MIN_KLINE_START_DATE,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        start = _parse_date(
            normalize_kline_start_date(start_date),
            "start_date",
        )
        end = date.today() if end_date is None else _parse_date(end_date, "end_date")
        if start > end:
            raise ValueError("start_date 不能晚于 end_date")

        current_end = end
        pages: list[list[object]] = []
        for page_number in range(1, TENCENT_MAX_PAGES + 1):
            rows = TencentKlineFetcher.fetch_page(
                tencent_symbol, current_end.isoformat(), adjust
            )
            if not rows:
                break
            pages = rows + pages
            first_date = _parse_row_date(rows[0], symbol=tencent_symbol, row_number=1)
            if len(rows) < TENCENT_PAGE_SIZE or first_date <= start:
                break
            next_end = first_date - timedelta(days=1)
            if next_end >= current_end:
                raise TencentKlineFetchError(
                    f"腾讯 K 线分页未向前推进: {tencent_symbol}, page={page_number}"
                )
            current_end = next_end
        else:
            raise TencentKlineFetchError(
                f"腾讯 K 线超过最大分页数，历史可能不完整: {tencent_symbol}"
            )

        parsed = _parse_rows(pages, symbol=tencent_symbol, adjust=adjust)
        if parsed.empty:
            return parsed
        return parsed[(parsed["date"] >= start) & (parsed["date"] <= end)].reset_index(
            drop=True
        )

    @staticmethod
    def fetch_full(
        symbol: str,
        *,
        start_date: str = MIN_KLINE_START_DATE,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        tencent_symbol = to_sina_symbol(symbol)
        raw = TencentKlineFetcher.fetch_series(
            tencent_symbol,
            adjust="",
            start_date=start_date,
            end_date=end_date,
        )
        if raw.empty:
            empty = pd.DataFrame(columns=TENCENT_OUTPUT_COLUMNS)
            empty.attrs["source"] = "tencent-newfq"
            empty.attrs["amount_source"] = "tencent-newfq"
            empty.attrs["amount_unit"] = "yuan"
            empty.attrs["weekend_rows_filtered"] = 0
            empty.attrs["known_bad_rows_filtered"] = 0
            return empty

        hfq = TencentKlineFetcher.fetch_series(
            tencent_symbol,
            adjust="hfq",
            start_date=start_date,
            end_date=end_date,
        )
        if hfq.empty:
            raise TencentKlineFetchError(f"腾讯 hfq 数据为空: {symbol}")

        hfq_source_rows = len(hfq)
        raw, raw_weekend_dates = _drop_weekend_rows(raw)
        hfq, hfq_weekend_dates = _drop_weekend_rows(hfq)
        raw, raw_bad_dates = drop_known_bad_kline_rows(raw, symbol)
        hfq, hfq_bad_dates = drop_known_bad_kline_rows(hfq, symbol)
        raw_dates = set(raw["date"])
        hfq_dates = set(hfq["date"])
        if raw_dates != hfq_dates:
            raise TencentKlineFetchError(
                f"腾讯 raw/hfq 日期集合不一致: {symbol}, "
                f"raw_only={len(raw_dates - hfq_dates)}, "
                f"hfq_only={len(hfq_dates - raw_dates)}"
            )
        weekend_rows_filtered = len(raw_weekend_dates | hfq_weekend_dates)
        known_bad_rows_filtered = len(raw_bad_dates | hfq_bad_dates)

        frame = raw.merge(
            hfq[["date", "close_hfq"]],
            on="date",
            how="left",
            validate="one_to_one",
        )
        if (frame["close"] <= 0).any() or (frame["close_hfq"] <= 0).any():
            raise TencentKlineFetchError(f"腾讯 raw/hfq 收盘价必须大于 0: {symbol}")
        frame["adj_factor"] = frame["close_hfq"] / frame["close"]
        frame = frame[
            [
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "close_hfq",
                "adj_factor",
            ]
        ]
        try:
            frame = validate_kline_frame(frame)
        except Exception as exc:
            raise TencentKlineFetchError(
                f"腾讯 K 线质量校验失败: {symbol}: {exc}"
            ) from exc
        frame.attrs["source"] = "tencent-newfq"
        frame.attrs["amount_source"] = "tencent-newfq"
        frame.attrs["amount_unit"] = "yuan"
        frame.attrs["weekend_rows_filtered"] = weekend_rows_filtered
        frame.attrs["known_bad_rows_filtered"] = known_bad_rows_filtered
        frame.attrs["hfq_source_rows"] = hfq_source_rows
        frame.attrs["hfq_forward_filled_rows"] = 0
        return frame
