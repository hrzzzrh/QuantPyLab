"""Sina klc_kl.js 底层数据获取工具

绕过 akshare stock_zh_a_daily 的 StockService.getAmountBySymbol 缺陷 (CDR/退市股
返回 null 导致 JSONDecodeError), 直接调用新浪底层接口。全静态方法, 无状态,
K 线采集、退市股重建、元数据补全等场景可任意复用。
"""

from ast import literal_eval
from datetime import datetime
from math import isfinite

import pandas as pd

from config.settings import MIN_KLINE_START_DATE
from utils.financial import to_sina_symbol
from utils.kline_policy import drop_known_bad_kline_rows, normalize_kline_start_date
from utils.requests_protection import SinaBlockedError

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

_KLC_RAW_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]
_KLC_OUTPUT_COLUMNS = [
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


def _prepare_raw_dates(
    data: list[dict], sina_symbol: str
) -> tuple[pd.DataFrame, pd.Series] | None:
    if not data:
        return None
    try:
        df_raw = pd.DataFrame(data)
        if "date" not in df_raw.columns:
            raise ValueError("原始数据缺少 date 字段")
        parsed_dates = pd.to_datetime(df_raw["date"], errors="coerce")
    except Exception as exc:
        raise SinaKlcFetchError(
            f"新浪 KLC 原始数据日期解析失败: {sina_symbol}: {exc}"
        ) from exc
    if parsed_dates.isna().any():
        raise SinaKlcFetchError(f"新浪 KLC 原始数据包含无效日期: {sina_symbol}")
    return df_raw, parsed_dates


class SinaHfqFetchError(RuntimeError):
    """新浪复权因子请求或响应解析失败。"""


class SinaKlcFetchError(RuntimeError):
    """新浪 KLC 原始行情请求或响应失败。"""


class SinaKlcFetcher:
    """Sina klc_kl.js 数据获取器 (静态方法, 任意场景可调用)"""

    @staticmethod
    def fetch_raw(sina_symbol: str) -> list[dict]:
        """GET klc_kl.js → JS 解密 → 返回原始 dict list

        :param sina_symbol: 新浪格式代码 (如 sh600519 / bj920305)
        :return: [{date, open, high, low, close, volume, amount, ...}, ...] 或 []
        """
        from unittest.mock import patch

        import py_mini_racer
        import requests
        from akshare.stock.cons import hk_js_decode, zh_sina_a_stock_hist_url

        original_get = requests.get

        def patched_get(*args, **kwargs):
            if "headers" not in kwargs or not kwargs["headers"]:
                kwargs["headers"] = {"User-Agent": _USER_AGENT}
            return original_get(*args, **kwargs)

        try:
            with patch("requests.get", side_effect=patched_get):
                r = requests.get(
                    zh_sina_a_stock_hist_url.format(sina_symbol), timeout=30
                )
                if r.status_code == 456:
                    raise SinaBlockedError(
                        f"新浪 KLC 原始数据触发风控: {sina_symbol}: HTTP 456"
                    )
                if r.status_code != 200:
                    raise SinaKlcFetchError(
                        f"获取新浪 KLC 原始数据失败: {sina_symbol}: HTTP {r.status_code}"
                    )
                js_code = py_mini_racer.MiniRacer()
                js_code.eval(hk_js_decode)
                raw_str = r.text.split("=")[1].split(";")[0].replace('"', "")
                return js_code.call("d", raw_str) or []
        except (SinaBlockedError, SinaKlcFetchError):
            raise
        except Exception as exc:
            raise SinaKlcFetchError(
                f"获取新浪 KLC 原始数据失败: {sina_symbol}: {exc}"
            ) from exc

    @staticmethod
    def fetch_hfq(sina_symbol: str) -> pd.DataFrame | None:
        """GET hfq.js → 返回复权因子 DataFrame (date, hfq_factor)

        :return: 接口明确返回无因子记录时返回 None
        :raises SinaHfqFetchError: 请求失败或响应格式无效
        """
        import requests
        from akshare.stock.cons import zh_sina_a_stock_hfq_url

        try:
            response = requests.get(
                zh_sina_a_stock_hfq_url.format(sina_symbol),
                headers={"User-Agent": _USER_AGENT},
                timeout=10,
            )
            if response.status_code == 456:
                raise SinaBlockedError(f"新浪复权因子触发风控: {sina_symbol}: HTTP 456")
            if response.status_code != 200:
                raise ValueError(f"HTTP {response.status_code}")
            hfq_json = literal_eval(response.text.split("=")[1].split("\n")[0])
            if not isinstance(hfq_json, dict) or "total" not in hfq_json:
                raise ValueError("响应缺少 total 字段")

            total = hfq_json["total"]
            if isinstance(total, bool) or not isinstance(total, int) or total < 0:
                raise ValueError("total 必须是非负整数")
            data = hfq_json.get("data")
            if total == 0:
                if data != []:
                    raise ValueError("total=0 时 data 必须是空列表")
                return None

            if not isinstance(data, list) or len(data) != total:
                raise ValueError("total 与 data 行数不一致")

            hfq_df = pd.DataFrame(data)
            if not {"d", "f"}.issubset(hfq_df.columns):
                raise ValueError("data 缺少 d/f 字段")
            hfq_df = hfq_df[["d", "f"]].rename(columns={"d": "date", "f": "hfq_factor"})
            hfq_df["hfq_factor"] = pd.to_numeric(hfq_df["hfq_factor"], errors="raise")
            if not hfq_df["hfq_factor"].map(isfinite).all():
                raise ValueError("复权因子包含非有限数值")
            parsed_dates = pd.to_datetime(hfq_df["date"], errors="raise")
            if parsed_dates.isna().any():
                raise ValueError("复权因子包含无效日期")
            hfq_df["date"] = parsed_dates.dt.strftime("%Y-%m-%d")
            if hfq_df["date"].duplicated().any():
                raise ValueError("复权因子包含重复日期")
            if (hfq_df["hfq_factor"] <= 0).any():
                raise ValueError("复权因子必须大于 0")
            hfq_df.sort_values("date", inplace=True)
            return hfq_df
        except SinaBlockedError:
            raise
        except Exception as exc:
            raise SinaHfqFetchError(
                f"获取新浪复权因子失败: {sina_symbol}: {exc}"
            ) from exc

    @staticmethod
    def fetch_klc_data(
        sina_symbol: str,
        start_date: str = MIN_KLINE_START_DATE,
        end_date: str = None,
    ) -> pd.DataFrame:
        """全量 K 线: raw + hfq 合并 → 标准输出

        :return: columns = [date, open, high, low, close, volume, amount,
                            close_hfq, adj_factor] (volume 单位: 手)
        """
        data = SinaKlcFetcher.fetch_raw(sina_symbol)
        if not data:
            return pd.DataFrame(columns=_KLC_OUTPUT_COLUMNS)

        prepared = _prepare_raw_dates(data, sina_symbol)
        if prepared is None:
            return pd.DataFrame(columns=_KLC_OUTPUT_COLUMNS)
        df_raw, parsed_dates = prepared
        missing_columns = [
            column for column in _KLC_RAW_COLUMNS if column not in df_raw.columns
        ]
        if missing_columns:
            raise SinaKlcFetchError(
                f"新浪 KLC 原始数据缺少字段: {', '.join(missing_columns)}"
            )
        df_raw = df_raw[_KLC_RAW_COLUMNS].copy()
        df_raw.index = parsed_dates.dt.date
        start_dt = datetime.strptime(
            normalize_kline_start_date(start_date), "%Y%m%d"
        ).date()
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y%m%d").date()
            df_raw = df_raw[start_dt:end_dt].copy()
        else:
            df_raw = df_raw[start_dt:].copy()
        if df_raw.empty:
            return pd.DataFrame(columns=_KLC_OUTPUT_COLUMNS)
        del df_raw["date"]

        try:
            df_raw = df_raw.astype("float")
        except (OverflowError, TypeError, ValueError) as exc:
            raise SinaKlcFetchError(
                f"新浪 KLC 原始数据包含非数值字段: {sina_symbol}"
            ) from exc
        numeric_columns = [column for column in _KLC_RAW_COLUMNS if column != "date"]
        if (
            not df_raw[numeric_columns]
            .apply(lambda column: column.map(isfinite).all())
            .all()
        ):
            raise SinaKlcFetchError(f"新浪 KLC 原始数据包含非有限数值: {sina_symbol}")
        df_raw["volume"] = df_raw["volume"] / 100.0
        df_raw.reset_index(inplace=True)
        df_raw["date"] = pd.to_datetime(df_raw["date"]).dt.strftime("%Y-%m-%d")

        hfq_df = SinaKlcFetcher.fetch_hfq(sina_symbol)
        if hfq_df is not None:
            raw_start = df_raw["date"].min()
            initial_factors = hfq_df[hfq_df["date"] <= raw_start]
            if initial_factors.empty:
                raise SinaHfqFetchError(f"复权因子未覆盖 raw 起始日期: {sina_symbol}")
            df_hfq = df_raw[["date", "close"]].copy()
            df_hfq = df_hfq.merge(hfq_df, on="date", how="left")
            direct_factor_mask = df_hfq["hfq_factor"].notna()
            initial_factor = initial_factors.iloc[-1]["hfq_factor"]
            first_date_mask = df_hfq["date"] == raw_start
            if df_hfq.loc[first_date_mask, "hfq_factor"].isna().all():
                df_hfq.loc[first_date_mask, "hfq_factor"] = initial_factor
            df_hfq["hfq_factor"] = df_hfq["hfq_factor"].ffill()
            if df_hfq["hfq_factor"].isna().any():
                raise SinaHfqFetchError(
                    f"复权因子存在无法解释的日期缺口: {sina_symbol}"
                )
            df_hfq["close_hfq"] = df_hfq["close"] * df_hfq["hfq_factor"]
            df_hfq = df_hfq[["date", "close_hfq", "hfq_factor"]]
            hfq_source_rows = len(hfq_df)
            hfq_forward_filled_rows = int((~direct_factor_mask).sum())
        else:
            # 只有新浪明确返回 total=0 时才允许按不复权价格处理。
            df_hfq = df_raw[["date", "close"]].rename(columns={"close": "close_hfq"})
            hfq_source_rows = 0
            hfq_forward_filled_rows = 0

        df_hfq["date"] = pd.to_datetime(df_hfq["date"]).dt.strftime("%Y-%m-%d")
        df_merge = pd.merge(df_raw, df_hfq, on="date", how="left")
        if hfq_df is None:
            df_merge["adj_factor"] = 1.0
        else:
            df_merge["adj_factor"] = df_merge["hfq_factor"]
            df_merge.drop(columns=["hfq_factor"], inplace=True)
        df_merge.attrs["hfq_source_rows"] = hfq_source_rows
        df_merge.attrs["hfq_forward_filled_rows"] = hfq_forward_filled_rows
        df_merge.attrs["source"] = "sina-klc"
        df_merge.attrs["amount_source"] = "sina-klc"
        df_merge.attrs["amount_unit"] = "yuan"
        df_merge, excluded_dates = drop_known_bad_kline_rows(df_merge, sina_symbol[2:])
        df_merge.attrs["known_bad_rows_filtered"] = len(excluded_dates)
        return df_merge.reindex(columns=_KLC_OUTPUT_COLUMNS)

    @staticmethod
    def fetch_list_date(code: str) -> str | None:
        """查询上市日期: klc_kl.js 首条记录日期

        :param code: 6 位数字代码
        :return: YYYYMMDD 或 None
        """
        sina_symbol = to_sina_symbol(code)
        data = SinaKlcFetcher.fetch_raw(sina_symbol)
        prepared = _prepare_raw_dates(data, sina_symbol)
        if prepared is None:
            return None
        _, parsed_dates = prepared
        return parsed_dates.iloc[0].strftime("%Y%m%d")
