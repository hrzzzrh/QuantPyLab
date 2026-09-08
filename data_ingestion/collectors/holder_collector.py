import calendar
import random
import time
from datetime import date, datetime

import akshare as ak
import pandas as pd

from storage.database.manager import db_manager
from storage.database.sync_status import (
    DATASET_HOLDER_NUMBER,
    record_sync_success,
)
from storage.file_store.parquet_store import ParquetStore
from utils.logger import logger
from utils.retry import retry

HOLDER_CATEGORY = "holder_number"
LOCAL_EPOCH = "1990-01-01"
CNINFO_START = date(2017, 3, 31)

# 东财个股明细源列名 → 规范列名
_SOURCE_COLUMNS = {
    "股东户数统计截止日": "stat_date",
    "股东户数-本次": "holder_count",
    "股东户数-上次": "prev_holder_count",
    "股东户数-增减": "holder_change",
    "股东户数-增减比例": "holder_change_pct",
    "股东户数公告日期": "announce_date",
    "代码": "source_code",
}

# 巨潮季度统计源列名 → 规范列名 (无公告日期, 见 _statutory_announce_date)
_CNINFO_SOURCE_COLUMNS = {
    "证券代码": "source_code",
    "变动日期": "stat_date",
    "本期股东人数": "holder_count",
    "上期股东人数": "prev_holder_count",
    "股东人数增幅": "holder_change_pct",
}

# 入库规范列 (symbol 为分区键, 单独补列; source 标记数据来源)
HOLDER_COLUMNS = (
    "stat_date",
    "holder_count",
    "prev_holder_count",
    "holder_change",
    "holder_change_pct",
    "announce_date",
)
OUTPUT_COLUMNS = ("symbol", *HOLDER_COLUMNS, "source")
SOURCE_EM = "em"
SOURCE_CNINFO = "cninfo"

_COUNT_COLUMNS = ("holder_count", "prev_holder_count", "holder_change")


def normalize_holder_detail(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """归一化东财个股股东户数明细为入库规范 (纯函数, 便于单测)。

    - 股票代码补齐 6 位 (源接口近期行会返回截断代码如 '2594');
      与目标 symbol 不一致的行丢弃并告警。
    - 缺失 stat_date / holder_count / announce_date 的行丢弃
      (announce_date 缺失无法做无穿越 ASOF 关联)。
    - 以 stat_date 去重 (keep last) 并升序排列。
    - 结果打标 source='em'。
    """
    canonical = pd.DataFrame(columns=list(OUTPUT_COLUMNS))
    if frame is None or frame.empty:
        return canonical

    missing = set(_SOURCE_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError("股东人数明细缺少字段: " + ", ".join(sorted(missing)))

    normalized = frame.loc[:, list(_SOURCE_COLUMNS)].rename(columns=_SOURCE_COLUMNS)
    normalized = normalized.copy()
    normalized["source_code"] = (
        normalized["source_code"]
        .astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )
    mismatched = normalized["source_code"] != symbol
    if mismatched.any():
        logger.warning(
            f"{symbol} 股东人数明细含 {int(mismatched.sum())} 行异码数据, 已丢弃"
        )
        normalized = normalized.loc[~mismatched].copy()
    if normalized.empty:
        return canonical

    normalized["stat_date"] = pd.to_datetime(
        normalized["stat_date"], format="mixed", errors="coerce"
    ).dt.date
    normalized["announce_date"] = pd.to_datetime(
        normalized["announce_date"], format="mixed", errors="coerce"
    ).dt.date
    for col in (*_COUNT_COLUMNS, "holder_change_pct"):
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    before = len(normalized)
    normalized = normalized.dropna(
        subset=["stat_date", "holder_count", "announce_date"]
    )
    if len(normalized) < before:
        logger.warning(
            f"{symbol} 股东人数明细丢弃 {before - len(normalized)} 行缺失关键字段的数据"
        )
    if normalized.empty:
        return canonical

    normalized = normalized.sort_values("stat_date", kind="mergesort").drop_duplicates(
        subset=["stat_date"], keep="last"
    )
    normalized["symbol"] = symbol
    normalized["source"] = SOURCE_EM
    return normalized.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)


def _statutory_announce_date(stat_date: date) -> date:
    """巨潮行伪公告日期：法定披露截止日 (方向保守，只会偏旧、不会穿越)。"""
    if (stat_date.month, stat_date.day) == (3, 31):
        return date(stat_date.year, 4, 30)
    if (stat_date.month, stat_date.day) == (6, 30):
        return date(stat_date.year, 8, 31)
    if (stat_date.month, stat_date.day) == (9, 30):
        return date(stat_date.year, 10, 31)
    if (stat_date.month, stat_date.day) == (12, 31):
        return date(stat_date.year + 1, 4, 30)
    # 非标准季度防御：取次月最后一天
    year, month = stat_date.year, stat_date.month + 1
    if month > 12:
        year, month = year + 1, 1
    return date(year, month, calendar.monthrange(year, month)[1])


def normalize_cninfo_frame(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """归一化巨潮季度股东人数为入库规范 (纯函数, 便于单测)。

    与东财口径差异：无公告日期 (取法定披露截止日伪值)、无户数增减绝对值
    (由本期减上期推导)。结果打标 source='cninfo'。
    """
    canonical = pd.DataFrame(columns=list(OUTPUT_COLUMNS))
    if frame is None or frame.empty:
        return canonical

    missing = set(_CNINFO_SOURCE_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError("巨潮股东人数缺少字段: " + ", ".join(sorted(missing)))

    normalized = frame.loc[:, list(_CNINFO_SOURCE_COLUMNS)].rename(
        columns=_CNINFO_SOURCE_COLUMNS
    )
    normalized = normalized.copy()
    normalized["source_code"] = (
        normalized["source_code"]
        .astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )
    mismatched = normalized["source_code"] != symbol
    if mismatched.any():
        logger.warning(
            f"{symbol} 巨潮股东人数含 {int(mismatched.sum())} 行异码数据, 已丢弃"
        )
        normalized = normalized.loc[~mismatched].copy()
    if normalized.empty:
        return canonical

    normalized["stat_date"] = pd.to_datetime(
        normalized["stat_date"], format="mixed", errors="coerce"
    ).dt.date
    normalized["holder_count"] = pd.to_numeric(
        normalized["holder_count"], errors="coerce"
    )
    normalized["prev_holder_count"] = pd.to_numeric(
        normalized["prev_holder_count"], errors="coerce"
    )
    normalized["holder_change_pct"] = pd.to_numeric(
        normalized["holder_change_pct"], errors="coerce"
    )

    before = len(normalized)
    normalized = normalized.dropna(subset=["stat_date", "holder_count"])
    if len(normalized) < before:
        logger.warning(
            f"{symbol} 巨潮股东人数丢弃 {before - len(normalized)} 行缺失关键字段的数据"
        )
    if normalized.empty:
        return canonical

    normalized["holder_change"] = (
        normalized["holder_count"] - normalized["prev_holder_count"]
    )
    normalized["announce_date"] = normalized["stat_date"].map(_statutory_announce_date)
    normalized = normalized.sort_values("stat_date", kind="mergesort").drop_duplicates(
        subset=["stat_date"], keep="last"
    )
    normalized["symbol"] = symbol
    normalized["source"] = SOURCE_CNINFO
    return normalized.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)


def _parse_quarter(quarter: str) -> date:
    """季度字符串 (YYYYMMDD) 转日期。"""
    return datetime.strptime(quarter, "%Y%m%d").date()


def _sleep_between_cninfo_fetches():
    """巨潮季度快照连续抓取时的保守间隔 (单测可 stub)。"""
    time.sleep(random.uniform(1, 1.5))


def _last_completed_quarter_end(today: date) -> date:
    """最近一个已结束的季度末 (巨潮季度统计只覆盖完整季度)。"""
    for month, last_day in ((12, 31), (9, 30), (6, 30), (3, 31)):
        if (today.month, today.day) >= (month, last_day):
            return date(today.year, month, last_day)
    return date(today.year - 1, 12, 31)


def _required_cninfo_quarters(after: date, today: date = None) -> list:
    """列出 (after, 最近完整季末] 区间内的季度末 (YYYYMMDD 字符串)。"""
    today = today or date.today()
    end = _last_completed_quarter_end(today)
    quarters = []
    year, month = CNINFO_START.year, CNINFO_START.month
    while True:
        last_day = calendar.monthrange(year, month)[1]
        quarter_end = date(year, month, last_day)
        if quarter_end > end:
            break
        if quarter_end > after:
            quarters.append(quarter_end.strftime("%Y%m%d"))
        month += 3
        if month > 12:
            year, month = year + 1, month - 12
    return quarters


class HolderCollector:
    """
    股东人数采集器：主源东财个股明细 (stock_zh_a_gdhs_detail_em)，
    东财缺数股 (A+B/A+H/CDR 等) 回退到巨潮季度统计 (stock_hold_num_cninfo)。
    基于本地最大统计截止日的增量抓取。
    """

    def __init__(self, fetcher=None, cninfo_fetcher=None):
        self.store = ParquetStore()
        self.fetcher = fetcher or ak.stock_zh_a_gdhs_detail_em
        self.cninfo_fetcher = cninfo_fetcher or ak.stock_hold_num_cninfo
        self._cninfo_cache = {}

    def _get_local_max_stat_date(self, symbol: str) -> str:
        """获取本地已存储的最新统计截止日"""
        try:
            conn = db_manager.get_duckdb_conn()
            path = (
                self.store.base_dir
                / HOLDER_CATEGORY
                / f"symbol={symbol}"
                / "data.parquet"
            )
            if not path.exists():
                return LOCAL_EPOCH

            res = conn.execute(
                f"SELECT MAX(stat_date) FROM read_parquet('{path}')"
            ).fetchone()
            if res and res[0]:
                if isinstance(res[0], (datetime, pd.Timestamp)):
                    return res[0].strftime("%Y-%m-%d")
                return str(res[0])
            return LOCAL_EPOCH
        except Exception:
            return LOCAL_EPOCH

    def _fetch_holder_detail(self, symbol: str) -> pd.DataFrame:
        """调用源接口获取单股全量历史明细"""
        return self.fetcher(symbol)

    @retry(max_retries=2, delay=2.0)
    def _fetch_cninfo_quarter(self, quarter: str) -> pd.DataFrame:
        """按季度拉取巨潮全市场快照 (实例级缓存, 同一批次内缺数股共享)。

        缓存只保留成功结果；失败向上抛，由调用方决定整股重试。
        """
        if quarter not in self._cninfo_cache:
            _sleep_between_cninfo_fetches()
            self._cninfo_cache[quarter] = self.cninfo_fetcher(quarter)
        cached = self._cninfo_cache[quarter]
        return cached if cached is not None else pd.DataFrame()

    def _local_stat_dates(self, symbol: str) -> set:
        """读取本地已存的统计截止日集合 (用于巨潮补缺口判定)。"""
        try:
            path = (
                self.store.base_dir
                / HOLDER_CATEGORY
                / f"symbol={symbol}"
                / "data.parquet"
            )
            if not path.exists():
                return set()
            df = pd.read_parquet(path, columns=["stat_date"])
            return set(pd.to_datetime(df["stat_date"]).dt.date)
        except Exception:
            return set()

    def _collect_cninfo_quarters(self, symbol: str, quarters: list) -> pd.DataFrame:
        """从巨潮季度统计中提取指定季度的本股行 (已按 stat_date 去重排序)。"""
        wanted = {_parse_quarter(q) for q in quarters}
        frames = []
        for quarter in quarters:
            raw = self._fetch_cninfo_quarter(quarter)
            if raw is None or raw.empty:
                continue
            norm = normalize_cninfo_frame(raw, symbol)
            frames.append(norm[norm["stat_date"].isin(wanted)])
        if not frames:
            return pd.DataFrame(columns=list(OUTPUT_COLUMNS))
        combined = pd.concat(frames, ignore_index=True)
        if combined.empty:
            return pd.DataFrame(columns=list(OUTPUT_COLUMNS))
        return combined.sort_values("stat_date", kind="mergesort").drop_duplicates(
            subset=["stat_date"], keep="last"
        )

    @retry(max_retries=2, delay=2.0, fatal_exceptions=(ValueError,))
    def collect_holder_number(self, symbol: str, start_date: str = None):
        """
        同步单股股东人数记录
        :param symbol: 纯数字代码 (如 600519)
        :param start_date: 选填，强制抓取的起始日期 (YYYYMMDD 或 YYYY-MM-DD)
        """
        local_max = self._get_local_max_stat_date(symbol)
        if not start_date:
            start_date = local_max

        logger.debug(f"正在抓取股东人数: {symbol} (本地最新: {local_max})")

        # 1. 东财主源抓取并归一化
        df_em = pd.DataFrame(columns=list(OUTPUT_COLUMNS))
        try:
            df_em = normalize_holder_detail(self._fetch_holder_detail(symbol), symbol)
        except Exception:
            logger.warning(f"{symbol} 东财明细缺失，尝试巨潮 fallback")

        # 2. 过滤增量数据 (支持 YYYYMMDD 和 YYYY-MM-DD 格式)
        if isinstance(start_date, str):
            start_date = start_date.replace("-", "")
            try:
                start_dt = datetime.strptime(start_date, "%Y%m%d").date()
            except ValueError:
                logger.error(
                    f"无效的日期格式: {start_date}，请使用 YYYYMMDD 或 YYYY-MM-DD"
                )
                return
        else:
            start_dt = start_date

        merged_any = False
        df_filtered = df_em[df_em["stat_date"] > start_dt].copy()
        if not df_filtered.empty:
            self._save_incremental(df_filtered, symbol)
            merged_any = True

        # 3. 巨潮补缺：本地缺失的季度 (含尾部新增与中部缺口)，东财行优先。
        #    已落盘与本次东财全量中出现过的日期都不再拉取；显式 --start-date
        #    不约束补缺下界。
        try:
            known_dates = self._local_stat_dates(symbol)
            if not df_em.empty:
                known_dates |= set(df_em["stat_date"])
            needed = [
                q
                for q in _required_cninfo_quarters(date(1990, 1, 1))
                if _parse_quarter(q) not in known_dates
            ]
            df_cn = self._collect_cninfo_quarters(symbol, needed)
        except Exception:
            if not merged_any:
                raise
            logger.warning(
                f"{symbol} 巨潮 fallback 失败，已保留东财数据", exc_info=True
            )
            df_cn = pd.DataFrame(columns=list(OUTPUT_COLUMNS))
        if not df_cn.empty:
            self._save_incremental(df_cn, symbol)
            merged_any = True

        # 4. 两边皆空不记成功 (下次重跑补抓)，但不计失败：
        #    源缺数≠同步失败，计失败会导致无数据股票使 sync-all 永久 RETRYABLE。
        if not merged_any:
            logger.warning(f"{symbol} 股东人数无可用数据，留待下次重跑")
            return
        record_sync_success(DATASET_HOLDER_NUMBER, symbol, date.today())

    def _save_incremental(self, df_new: pd.DataFrame, symbol: str):
        """增量合并并保存"""
        path = (
            self.store.base_dir / HOLDER_CATEGORY / f"symbol={symbol}" / "data.parquet"
        )

        if path.exists():
            df_old = pd.read_parquet(path)
            # 确保类型一致
            df_old["stat_date"] = pd.to_datetime(df_old["stat_date"]).dt.date
            df_old["announce_date"] = pd.to_datetime(df_old["announce_date"]).dt.date

            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=["stat_date"], keep="last", inplace=True)
            df_combined.sort_values("stat_date", inplace=True)
        else:
            df_combined = df_new

        # 存量 EM 行无 source 列时回填 'em'
        if "source" not in df_combined.columns:
            df_combined["source"] = SOURCE_EM
        else:
            df_combined["source"] = df_combined["source"].fillna(SOURCE_EM)

        # 户数列用可空 Int64：未知 (NaN) 保持 NULL，禁止写成 0
        # (0 会被下游误读为“户数不变”，而 NULL 明确表示未知)
        for col in _COUNT_COLUMNS:
            df_combined[col] = pd.to_numeric(df_combined[col], errors="coerce").round()
            # round 后仍为 float64 (NaN 安全)；可空 Int64 保留 NULL 落 parquet
            df_combined[col] = df_combined[col].astype("Int64")
        df_combined["holder_change_pct"] = pd.to_numeric(
            df_combined["holder_change_pct"], errors="coerce"
        ).astype(float)

        self.store.save_partition(df_combined, HOLDER_CATEGORY, symbol)
        logger.debug(f"股东人数保存成功: {symbol} ({len(df_combined)} 条记录)")


if __name__ == "__main__":
    collector = HolderCollector()
    collector.collect_holder_number("600519")
