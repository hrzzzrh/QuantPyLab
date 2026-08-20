from contextlib import nullcontext
from datetime import date, datetime, timedelta

import akshare as ak
import pandas as pd

from config.settings import MIN_KLINE_START_DATE
from storage.database.manager import db_manager
from storage.database.sync_status import (
    DATASET_KLINE,
    DATASET_KLINE_DAILY,
    DATASET_KLINE_DAILY_NO_DATA,
    clear_sync_status,
    get_last_sync_date,
    is_synced_today,
    record_sync_success,
)
from storage.file_store.parquet_store import ParquetStore
from utils.canonical_write_lock import (
    CanonicalWriteLock,
    canonical_write_lock_held,
)
from utils.financial import MarketLabel, get_market_label, to_sina_symbol
from utils.kline_policy import drop_known_bad_kline_rows, normalize_kline_start_date
from utils.kline_validation import (
    STORED_COLUMNS,
    KlineValidationError,
    validate_kline_frame,
)
from utils.logger import logger
from utils.requests_protection import SinaBlockedError
from utils.retry import retry
from utils.sina_klc import SinaHfqFetchError, SinaKlcFetchError
from utils.tencent_kline import (
    TencentKlineFetcher,
    TencentKlineFetchError,
    TencentKlineTransientError,
)
from utils.trade_date import get_latest_trade_date


class KlineDataUnavailableError(RuntimeError):
    """Raised when a delisted stock has no source K-line data."""


class KlineDataTransientError(RuntimeError):
    """Raised when a delisted-stock source failure should be retried."""


class DailyKlineCollector:
    """
    日线 K 线采集器
    策略: 存原始价格 + 复权因子
    支持多源切换 (Sina/EM)
    """

    def __init__(self, source: str = "em"):
        if source not in {"em", "sina", "sina-klc"}:
            raise ValueError(f"不支持的日线数据源: {source!r}")
        self.store = ParquetStore()
        self.source = source
        self._pending_daily_status_symbols: set[str] = set()
        self._pending_daily_no_data_status_symbols: set[str] = set()
        self._pending_rebuild_status_symbols: set[str] = set()

    def _canonical_write_context(self, symbol: str):
        """Use the caller's lock or acquire one for read-modify-write operations."""
        if canonical_write_lock_held(self.store.base_dir):
            return nullcontext()
        return CanonicalWriteLock(
            self.store.base_dir,
            operation="collector-save-kline",
            run_id=symbol,
        )

    def _is_delisted(self, symbol: str) -> bool:
        """查询 stocks 表判断是否退市 (is_active=0)"""
        conn = db_manager.get_sqlite_conn()
        row = conn.execute(
            "SELECT is_active FROM stocks WHERE symbol = ?", (symbol,)
        ).fetchone()
        return row is not None and row[0] == 0

    def _get_local_max_date(self, symbol: str) -> str:
        """获取本地已存储的最新日期"""
        try:
            conn = db_manager.get_duckdb_conn()
            # 尝试从视图中查询，如果视图未建立或表为空则返回远古日期
            # 注意: daily_kline 视图可能尚未在 DBManager 中定义，这里直接读文件
            path = (
                self.store.base_dir
                / "daily_kline"
                / f"symbol={symbol}"
                / "data.parquet"
            )
            if not path.exists():
                return "19900101"

            res = conn.execute(
                f"SELECT MAX(date) FROM read_parquet('{path}')"
            ).fetchone()
            if res and res[0]:
                # DuckDB 返回 date 类型或 ISO 字符串
                if isinstance(res[0], datetime):
                    return res[0].strftime("%Y%m%d")
                return str(res[0]).replace("-", "")
            return "19900101"
        except Exception:
            return "19900101"

    def _fetch_from_em(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """从东方财富抓取行情 (原逻辑)"""
        # 1. 抓取不复权数据
        df_raw = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
        if df_raw.empty:
            return pd.DataFrame()

        # 2. 抓取后复权数据 (用于计算因子)
        df_hfq = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="hfq",
        )
        if df_hfq.empty:
            raise KlineValidationError("后复权数据为空")

        # 3. 对齐并计算 adj_factor
        rename_map = {
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
        }
        df_raw = df_raw[list(rename_map.keys())].rename(columns=rename_map)
        df_hfq = df_hfq[["日期", "收盘"]].rename(
            columns={"日期": "date", "收盘": "close_hfq"}
        )

        df_merge = pd.merge(df_raw, df_hfq, on="date", how="left")
        if df_merge["close_hfq"].isna().any():
            raise KlineValidationError("后复权数据未覆盖所有原始交易日期")
        df_merge["adj_factor"] = df_merge["close_hfq"] / df_merge["close"]

        return df_merge

    def _fetch_from_sina(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """从新浪财经抓取行情 (新逻辑)"""
        sina_symbol = to_sina_symbol(symbol)

        # 补丁: 新浪接口对 UA 敏感，akshare 内部请求可能缺失 UA 导致断开连接
        from unittest.mock import patch

        import requests

        original_get = requests.get

        def patched_get(*args, **kwargs):
            if "headers" not in kwargs or not kwargs["headers"]:
                kwargs["headers"] = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
                }
            return original_get(*args, **kwargs)

        with patch("requests.get", side_effect=patched_get):
            try:
                # 1. 抓取不复权数据
                df_raw = ak.stock_zh_a_daily(
                    symbol=sina_symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )
                if df_raw.empty:
                    return pd.DataFrame()

                # 2. 抓取后复权数据
                df_hfq = ak.stock_zh_a_daily(
                    symbol=sina_symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="hfq",
                )
            except SinaBlockedError:
                # IP 风控: 立即传播止损, 不得降级 CDR (同域接口, 降级等于再发一次被风控请求)
                raise
            except Exception as e:
                logger.warning(
                    f"{symbol} akshare 新浪解析失败 ({type(e).__name__}), "
                    f"切换 CDR 专用接口"
                )
                return self._fetch_cdr_sina(sina_symbol, start_date, end_date)

        # 3. 标准化处理
        # 新浪接口列名已经是英文: date, open, high, low, close, volume, amount, ...
        # 注意: 新浪 volume 单位是股，本项目统一为手
        df_raw["volume"] = df_raw["volume"] / 100.0

        df_raw = df_raw[["date", "open", "high", "low", "close", "volume", "amount"]]
        df_hfq = df_hfq[["date", "close"]].rename(columns={"close": "close_hfq"})

        # 确保日期格式一致以便合并
        df_raw["date"] = pd.to_datetime(df_raw["date"]).dt.strftime("%Y-%m-%d")
        df_hfq["date"] = pd.to_datetime(df_hfq["date"]).dt.strftime("%Y-%m-%d")

        df_merge = pd.merge(df_raw, df_hfq, on="date", how="left")
        if df_merge["close_hfq"].isna().any():
            raise KlineValidationError("后复权数据未覆盖所有原始交易日期")

        # 计算因子
        df_merge["adj_factor"] = df_merge["close_hfq"] / df_merge["close"]

        return df_merge

    def _fetch_sina_klc(
        self,
        sina_symbol: str,
        start_date: str = MIN_KLINE_START_DATE,
        end_date: str = None,
    ) -> pd.DataFrame:
        """直接调用 klc_kl.js 解密获取 K 线, 绕过 akshare StockService.getAmountBySymbol 缺陷"""
        from utils.sina_klc import SinaKlcFetcher

        frame = SinaKlcFetcher.fetch_klc_data(sina_symbol, start_date, end_date)
        frame.attrs["source"] = "sina-klc"
        return frame

    def _fetch_cdr_sina(
        self, sina_symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """CDR 股票专用 fallback"""
        return self._fetch_sina_klc(sina_symbol, start_date, end_date)

    def _fetch_tencent_newfq(self, symbol: str) -> pd.DataFrame:
        """Fetch a complete delisted-stock partition from Tencent."""
        return TencentKlineFetcher.fetch_full(
            symbol,
            start_date=MIN_KLINE_START_DATE,
            end_date=get_latest_trade_date().strftime("%Y%m%d"),
        )

    def _fetch_delisted_rebuild_frame(self, symbol: str) -> pd.DataFrame:
        """Prefer KLC, then rebuild the whole stock from Tencent if needed."""
        sina_symbol = to_sina_symbol(symbol)
        sina_error_message = ""
        try:
            frame = self._fetch_sina_klc(sina_symbol)
            frame, _ = drop_known_bad_kline_rows(frame, symbol)
            if frame.empty:
                raise KlineDataUnavailableError("新浪 KLC 返回空数据")
            return validate_kline_frame(frame)
        except SinaBlockedError:
            raise
        except (
            KlineDataUnavailableError,
            KlineValidationError,
            SinaHfqFetchError,
            SinaKlcFetchError,
        ) as sina_error:
            logger.warning(
                "%s 退市股新浪 KLC 重建失败，切换腾讯整段重建: %s",
                symbol,
                sina_error,
            )
            sina_error_message = str(sina_error)

        try:
            frame = self._fetch_tencent_newfq(symbol)
            frame, _ = drop_known_bad_kline_rows(frame, symbol)
            if frame.empty:
                return frame
            return validate_kline_frame(frame)
        except TencentKlineTransientError as tencent_error:
            raise KlineDataTransientError(
                f"{symbol} 退市股新浪 KLC 与腾讯 newfqkline 暂时不可用; "
                f"新浪: {sina_error_message}; 腾讯: {tencent_error}"
            ) from tencent_error
        except (
            KlineDataUnavailableError,
            KlineValidationError,
            TencentKlineFetchError,
        ) as tencent_error:
            raise KlineDataUnavailableError(
                f"{symbol} 退市股新浪 KLC 与腾讯 newfqkline 均不可用; "
                f"新浪: {sina_error_message}; 腾讯: {tencent_error}"
            ) from tencent_error

    def _preserve_pre_cutoff_rows(
        self, frame: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        """Keep existing pre-cutoff rows when rebuilding a delisted partition."""
        path = self.store.base_dir / "daily_kline" / f"symbol={symbol}" / "data.parquet"
        if not path.exists():
            return frame

        old = pd.read_parquet(path)
        if old.empty:
            return frame
        cutoff = datetime.strptime(MIN_KLINE_START_DATE, "%Y%m%d").date()
        old["date"] = pd.to_datetime(old["date"]).dt.date
        preserved = old[old["date"] < cutoff]
        if preserved.empty:
            return frame

        current = frame[list(STORED_COLUMNS)].copy()
        current["date"] = pd.to_datetime(current["date"]).dt.date
        combined = pd.concat(
            [preserved[list(STORED_COLUMNS)], current],
            ignore_index=True,
        )
        combined.drop_duplicates(subset=["date"], keep="last", inplace=True)
        return combined.sort_values("date").reset_index(drop=True)

    def _record_daily_success(self, symbol: str) -> None:
        try:
            record_sync_success(DATASET_KLINE_DAILY, symbol, date.today())
        except Exception:
            self._pending_daily_status_symbols.add(symbol)
            logger.warning("K 线已保存但同步状态写入失败: %s", symbol)
            raise
        try:
            clear_sync_status(DATASET_KLINE_DAILY_NO_DATA, symbol)
        except Exception:
            self._pending_daily_status_symbols.add(symbol)
            logger.warning("K 线成功状态已写入但旧无行情状态清理失败: %s", symbol)
            raise
        self._pending_daily_status_symbols.discard(symbol)

    def _record_daily_no_data(self, symbol: str) -> None:
        try:
            record_sync_success(DATASET_KLINE_DAILY_NO_DATA, symbol, date.today())
        except Exception:
            self._pending_daily_no_data_status_symbols.add(symbol)
            logger.warning("无行情但冷却状态写入失败: %s", symbol)
            raise
        self._pending_daily_no_data_status_symbols.discard(symbol)

    def _record_rebuild_success(self, symbol: str) -> None:
        try:
            record_sync_success(DATASET_KLINE, symbol, date.today())
        except Exception:
            self._pending_rebuild_status_symbols.add(symbol)
            logger.warning("退市股 K 线已保存但同步状态写入失败: %s", symbol)
            raise
        try:
            clear_sync_status(DATASET_KLINE_DAILY_NO_DATA, symbol)
        except Exception:
            self._pending_rebuild_status_symbols.add(symbol)
            logger.warning("退市股成功重建但旧无行情状态清理失败: %s", symbol)
            raise
        self._pending_rebuild_status_symbols.discard(symbol)

    def _filter_before_minimum_date(
        self, frame: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        if frame.empty:
            return frame
        cutoff = datetime.strptime(MIN_KLINE_START_DATE, "%Y%m%d").date()
        dates = pd.to_datetime(frame["date"])
        filtered = frame.loc[dates.dt.date >= cutoff].reset_index(drop=True)
        filtered.attrs = frame.attrs.copy()
        filtered, _ = drop_known_bad_kline_rows(filtered, symbol)
        return filtered

    @retry(
        max_retries=2,
        delay=2.0,
        fatal_exceptions=(SinaBlockedError, KlineDataUnavailableError),
    )
    def collect_kline(self, symbol: str, start_date: str = None, end_date: str = None):
        """
        同步日线行情
        :param symbol: 纯数字代码 (如 600519)
        :return: True 表示实际抓取了数据, False 表示已是最新无需同步
        """
        if symbol in self._pending_rebuild_status_symbols:
            self._record_rebuild_success(symbol)
            return True
        if symbol in self._pending_daily_status_symbols:
            self._record_daily_success(symbol)
            return True
        if symbol in self._pending_daily_no_data_status_symbols:
            self._record_daily_no_data(symbol)
            return False

        # 退市股: Sina klc_kl.js 全量重建, 建后永久跳过
        if self._is_delisted(symbol):
            if get_last_sync_date(DATASET_KLINE, symbol) is not None or is_synced_today(
                DATASET_KLINE_DAILY_NO_DATA, symbol
            ):
                return False
            df = self._fetch_delisted_rebuild_frame(symbol)
            df = self._filter_before_minimum_date(df, symbol)
            if df.empty:
                logger.info("%s 最低起始日后无 K 线数据，记录无行情状态", symbol)
                self._record_daily_no_data(symbol)
                return False
            with self._canonical_write_context(symbol):
                df = self._preserve_pre_cutoff_rows(df, symbol)
                self.store.save_partition(
                    df[list(STORED_COLUMNS)], "daily_kline", symbol
                )
            last_date = df["date"].max()
            last_date_str = (
                last_date.strftime("%Y%m%d")
                if hasattr(last_date, "strftime")
                else str(last_date).replace("-", "")
            )
            conn = db_manager.get_sqlite_conn()
            conn.execute(
                "UPDATE stocks SET last_trade_date = ? WHERE symbol = ?",
                (last_date_str, symbol),
            )
            conn.commit()
            self._record_rebuild_success(symbol)
            logger.info(f"{symbol} 退市股 K线全量重建完成: {len(df)} 条")
            return True

        # 获取最新的有效交易日作为基准
        latest_trade_date = get_latest_trade_date().strftime("%Y%m%d")
        automatic_start_date = start_date is None

        if not end_date:
            end_date = latest_trade_date

        if not start_date:
            local_max = self._get_local_max_date(symbol)
            if local_max == "19900101":
                start_date = "19900101"
            else:
                # 增量同步: 从本地最大日期的后一天开始
                dt = datetime.strptime(local_max, "%Y%m%d") + timedelta(days=1)
                start_date = dt.strftime("%Y%m%d")
        start_date = normalize_kline_start_date(start_date)

        local_partition_path = (
            self.store.base_dir / "daily_kline" / f"symbol={symbol}" / "data.parquet"
        )
        if start_date > end_date:
            if (
                automatic_start_date
                and end_date == latest_trade_date
                and local_partition_path.exists()
            ):
                # Rebuild status after a process crash between Parquet and SQLite writes.
                self._record_daily_success(symbol)
            logger.debug(f"{symbol} 已是最新 (目标: {end_date})，无需同步")
            return False

        if automatic_start_date and (
            is_synced_today(DATASET_KLINE_DAILY, symbol)
            or is_synced_today(DATASET_KLINE_DAILY_NO_DATA, symbol)
        ):
            logger.debug(f"{symbol} 当日已尝试同步 (停牌/无交易)，跳过")
            return False

        # --- 数据源路由 ---
        market = get_market_label(symbol)
        if market == MarketLabel.BJ:
            active_source = "sina-klc" if self.source == "sina-klc" else "sina"
        else:
            active_source = "sina-klc" if self.source == "sina-klc" else "sina"

        logger.debug(
            f"正在从 {active_source} 抓取行情: {symbol} ({start_date} -> {end_date})"
        )
        try:
            if active_source == "sina-klc":
                df_merge = self._fetch_sina_klc(
                    to_sina_symbol(symbol), start_date, end_date
                )
            elif active_source == "sina":
                df_merge = self._fetch_from_sina(symbol, start_date, end_date)
            else:
                df_merge = self._fetch_from_em(symbol, start_date, end_date)
        except Exception as exc:
            logger.warning(
                f"行情抓取失败: {symbol}，active_source:{active_source}: {exc}"
            )
            raise

        df_merge = self._filter_before_minimum_date(df_merge, symbol)
        if df_merge.empty:
            logger.warning(f"{symbol} 抓取数据为空 (Source: {active_source})，可能停牌")
            self._record_daily_no_data(symbol)
            return False

        if active_source == "sina-klc" or df_merge.attrs.get("source") == "sina-klc":
            df_merge = validate_kline_frame(df_merge)

        try:
            # 最终清洗
            df_merge["symbol"] = symbol
            df_merge["date"] = pd.to_datetime(
                df_merge["date"]
            ).dt.date  # 转为 Python date 对象

            final_cols = [
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "adj_factor",
                "symbol",
            ]
            df_final = df_merge[final_cols].copy()

            # 5. 存储 (增量合并逻辑)
            self._save_incremental(df_final, symbol)
        except Exception as exc:
            logger.warning(f"行情清洗或保存失败: {symbol}: {exc}")
            raise

        self._record_daily_success(symbol)
        return True

    def _save_incremental(self, df_new: pd.DataFrame, symbol: str):
        """增量合并并保存"""
        with self._canonical_write_context(symbol):
            path = (
                self.store.base_dir
                / "daily_kline"
                / f"symbol={symbol}"
                / "data.parquet"
            )

            if path.exists():
                df_old = pd.read_parquet(path)
                # 简单追加并去重
                df_combined = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_combined = df_new

            df_combined, _ = drop_known_bad_kline_rows(df_combined, symbol)
            df_combined.drop_duplicates(subset=["date"], keep="last", inplace=True)
            df_combined.sort_values("date", inplace=True)
            self.store.save_partition(df_combined, "daily_kline", symbol)
            logger.debug(f"行情保存成功: {symbol} ({len(df_combined)} 条记录)")


if __name__ == "__main__":
    # 测试
    collector = DailyKlineCollector(source="sina-klc")
    collector.collect_kline("002859")
