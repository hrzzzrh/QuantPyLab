"""Sina klc_kl.js 底层数据获取工具

绕过 akshare stock_zh_a_daily 的 StockService.getAmountBySymbol 缺陷 (CDR/退市股
返回 null 导致 JSONDecodeError), 直接调用新浪底层接口。全静态方法, 无状态,
K 线采集、退市股重建、元数据补全等场景可任意复用。
"""

from datetime import datetime

import pandas as pd

from utils.financial import to_sina_symbol

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

_SINA_EXTRA_COLS = ["prevclose", "postVol", "postAmt"]


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

        with patch("requests.get", side_effect=patched_get):
            r = requests.get(zh_sina_a_stock_hist_url.format(sina_symbol))
            js_code = py_mini_racer.MiniRacer()
            js_code.eval(hk_js_decode)
            raw_str = r.text.split("=")[1].split(";")[0].replace('"', "")
            return js_code.call("d", raw_str) or []

    @staticmethod
    def fetch_hfq(sina_symbol: str) -> pd.DataFrame | None:
        """GET hfq.js → 返回复权因子 DataFrame (date, hfq_factor)

        :return: 无因子时返回 None
        """
        import requests
        from akshare.stock.cons import zh_sina_a_stock_hfq_url

        try:
            r = requests.get(
                zh_sina_a_stock_hfq_url.format(sina_symbol),
                headers={"User-Agent": _USER_AGENT},
                timeout=10,
            )
            hfq_json = eval(r.text.split("=")[1].split("\n")[0])
        except Exception:
            return None
        if not hfq_json.get("total", 0) > 0:
            return None
        hfq_df = pd.DataFrame(hfq_json["data"])
        hfq_df.columns = ["date", "hfq_factor"]
        hfq_df["hfq_factor"] = pd.to_numeric(hfq_df["hfq_factor"])
        hfq_df["date"] = pd.to_datetime(hfq_df["date"]).dt.strftime("%Y-%m-%d")
        return hfq_df

    @staticmethod
    def fetch_klc_data(
        sina_symbol: str, start_date: str = "19900101", end_date: str = None
    ) -> pd.DataFrame:
        """全量 K 线: raw + hfq 合并 → 标准输出

        :return: columns = [date, open, high, low, close, volume, amount,
                            close_hfq, adj_factor] (volume 单位: 手)
        """
        data = SinaKlcFetcher.fetch_raw(sina_symbol)
        if not data:
            return pd.DataFrame()

        df_raw = pd.DataFrame(data)
        df_raw.index = pd.to_datetime(df_raw["date"], errors="coerce").dt.date
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y%m%d").date()
            df_raw = df_raw[start_dt:end_dt].copy()
        else:
            df_raw = df_raw[start_dt:].copy()
        del df_raw["date"]

        for col in _SINA_EXTRA_COLS:
            if col in df_raw.columns:
                del df_raw[col]
        df_raw = df_raw.astype("float")
        df_raw["volume"] = df_raw["volume"] / 100.0
        df_raw.reset_index(inplace=True)
        df_raw["date"] = pd.to_datetime(df_raw["date"]).dt.strftime("%Y-%m-%d")

        hfq_df = SinaKlcFetcher.fetch_hfq(sina_symbol)
        if hfq_df is not None:
            df_hfq = df_raw[["date", "close"]].copy()
            df_hfq = df_hfq.merge(hfq_df, on="date", how="left")
            df_hfq["hfq_factor"] = df_hfq["hfq_factor"].ffill().fillna(1.0)
            df_hfq["close_hfq"] = df_hfq["close"] * df_hfq["hfq_factor"]
            df_hfq = df_hfq[["date", "close_hfq"]]
        else:
            df_hfq = df_raw[["date", "close"]].rename(columns={"close": "close_hfq"})

        df_hfq["date"] = pd.to_datetime(df_hfq["date"]).dt.strftime("%Y-%m-%d")
        df_merge = pd.merge(df_raw, df_hfq, on="date", how="left")
        df_merge["adj_factor"] = df_merge["close_hfq"] / df_merge["close"]
        df_merge["adj_factor"] = df_merge["adj_factor"].ffill().fillna(1.0)
        return df_merge

    @staticmethod
    def fetch_list_date(code: str) -> str | None:
        """查询上市日期: klc_kl.js 首条记录日期

        :param code: 6 位数字代码
        :return: YYYYMMDD 或 None
        """
        sina_symbol = to_sina_symbol(code)
        data = SinaKlcFetcher.fetch_raw(sina_symbol)
        if not data:
            return None
        first = data[0].get("date")
        if not first:
            return None
        return pd.to_datetime(first).strftime("%Y%m%d")
