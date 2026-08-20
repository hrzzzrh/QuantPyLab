import argparse
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from config.settings import MIN_KLINE_START_DATE
from data_ingestion.collectors.financial_collector import FinancialCollector
from data_ingestion.collectors.stock_list import (
    StockDetailCollector,
    StockListCollector,
)
from storage.database.financial_publish_date_verifier import (
    verify_overdue_financial_publish_dates_for_symbol,
)
from storage.database.financial_store import FinancialStore
from storage.database.indicator_store import IndicatorStore
from storage.database.manager import db_manager
from storage.database.official_disclosure_date_resolver import (
    OfficialDisclosureDateResolver,
)
from utils.canonical_write_lock import CanonicalWriteLock
from utils.financial import get_consecutive_reports
from utils.logger import logger
from utils.requests_protection import SinaBlockedError

# --- 辅助函数 ---


def get_all_stocks():
    """获取 stocks 表全部股票的 (code, name) (含已退市股, 保证从零重建时历史数据可回测)"""
    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT code, name FROM stocks")
    return cursor.fetchall()


def get_financial_symbols():
    """
    获取数据仓库中实际存在财务报表的全部股票代码。
    数据源为 fin_income_statement 视图 (Parquet 分片目录枚举)，
    覆盖 stocks 表之外的"孤儿股" (如已退市但历史数据保留的股票)。
    """
    conn = db_manager.get_duckdb_conn()
    db_manager.ensure_views("fin_income_statement")
    rows = conn.execute("SELECT DISTINCT symbol FROM fin_income_statement").fetchall()
    return [r[0] for r in rows]


def get_orphan_codes(category: str, all_codes: list) -> list:
    """获取尚未同步过特定类别数据的股票代码"""
    if category == "financial":
        from storage.database.financial_store import FinancialStore

        orphans = FinancialStore().get_stocks_without_financials(all_codes)
    elif category == "indicators":
        from storage.database.indicator_store import IndicatorStore

        orphans = IndicatorStore().get_stocks_without_indicators(all_codes)
    else:
        return []
    return orphans


# --- 业务逻辑函数 ---

# sync-all 流水线返回状态 (三态)
SYNC_ALL_SUCCESS = "success"
SYNC_ALL_RETRYABLE = "retryable"
SYNC_ALL_BLOCKED = "blocked"


def sync_stock_list():
    """同步股票列表: 差量 diff 更新 (新增插入, 存量更新, 消失标记退市)

    以 AkShare 当前在市列表为基准:
    - 新列表有、库中无   -> INSERT (is_active=1)
    - 两边都有          -> 更新 name (若曾被误标退市则恢复)
    - 库中有、新列表无   -> UPDATE is_active=0 (last_trade_date 保持 NULL,
      由退市股 K 线新浪 KLC 重建流程写入真实最后交易日)
    接口返回空列表时跳过, 防止数据源异常导致全库误标退市。

    返回 (processed, failed): processed=1, failed=0 表示名单同步成功
    (含退市清单合并), failed=1 表示退市清单接口异常。
    """
    collector = StockListCollector()
    df = collector.fetch_all_stocks()
    if df.empty:
        logger.error("股票列表接口返回为空, 跳过 diff 更新并判定失败 (防止误标退市)")
        return 1, 1

    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    existing = cursor.execute("SELECT symbol, name, is_active FROM stocks").fetchall()
    existing_map = {row[0]: (row[1], row[2]) for row in existing}
    incoming = set(df["symbol"].tolist())
    old_symbols = set(existing_map.keys())

    # 1. 新上市: 插入
    new_symbols = incoming - old_symbols
    if new_symbols:
        df_new = df[df["symbol"].isin(new_symbols)]
        df_new.to_sql("stocks", conn, if_exists="append", index=False)
        logger.info(f"新增 {len(df_new)} 只股票: {', '.join(new_symbols)}")

    # 2. 两边都有: 更新名称; 若曾被标记退市则恢复
    common = incoming & old_symbols
    restored = 0
    for symbol in common:
        row = df[df["symbol"] == symbol].iloc[0]
        old_name, old_active = existing_map[symbol]
        if row["name"] != old_name or old_active == 0:
            cursor.execute(
                "UPDATE stocks SET name = ?, is_active = 1, last_trade_date = NULL,"
                " updated_at = CURRENT_TIMESTAMP WHERE symbol = ?",
                (row["name"], symbol),
            )
            if old_active == 0:
                restored += 1
    if restored:
        logger.info(f"恢复 {restored} 只曾被标记退市的股票")

    # 3. 库中有、新列表无: 标记退市 + 回填 last_trade_date
    gone = old_symbols - incoming
    delisted = 0
    for symbol in gone:
        old_name, old_active = existing_map[symbol]
        if old_active == 0:
            continue
        cursor.execute(
            "UPDATE stocks SET is_active = 0, updated_at = CURRENT_TIMESTAMP"
            " WHERE symbol = ?",
            (symbol,),
        )
        logger.info(f"标记退市: {symbol} {old_name}")
        delisted += 1

    conn.commit()
    total = cursor.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    inactive_total = cursor.execute(
        "SELECT COUNT(*) FROM stocks WHERE is_active = 0"
    ).fetchone()[0]
    logger.info(
        f"stocks 表 diff 同步完成: 在市 {len(incoming)} 只"
        f" (本次新增 {len(new_symbols)}, 本次标记退市 {delisted}),"
        f" 库内总数 {total} (其中退市 {inactive_total})"
    )

    merge_ok = merge_delisted_stocks()
    return 1, 0 if merge_ok else 1


def merge_delisted_stocks() -> bool:
    """从沪深交易所退市股清单合并历史退市股 (仅插入缺失, 不修改已有行)

    重建场景必需: 当前在市列表不含历史退市股, 仅靠 diff 无法恢复退市股清单,
    会导致退市股数据 (K线/财务/股本) 永远无法同步。北交所退市股无清单接口, 暂不覆盖。

    返回 True 表示清单接口正常 (含正常返回空清单), False 表示接口异常 (跳过合并)。
    """
    import akshare as ak

    conn = db_manager.get_sqlite_conn()
    existing_codes = {r[0] for r in conn.execute("SELECT code FROM stocks").fetchall()}

    try:
        sh = ak.stock_info_sh_delist()
        sz = ak.stock_info_sz_delist()
    except Exception as e:
        logger.warning(f"退市股清单接口获取失败, 跳过合并: {e}")
        return False

    def _candidates(df, code_col, name_col, date_col):
        rows = []
        for _, row in df.iterrows():
            code = str(row[code_col]).zfill(6)
            if code in existing_codes:
                continue
            name = str(row[name_col])
            date_raw = row[date_col]
            last_date = str(date_raw).replace("-", "") if pd.notna(date_raw) else None
            rows.append((code, code, name, last_date))
        return rows

    candidates = []
    if sh is not None and not sh.empty:
        candidates += _candidates(sh, "公司代码", "公司简称", "暂停上市日期")
    if sz is not None and not sz.empty:
        candidates += _candidates(sz, "证券代码", "证券简称", "终止上市日期")

    # 按 code 去重 (沪深清单可能有重叠代码)
    seen = set()
    merged = 0
    for code, symbol, name, last_date in candidates:
        if code in seen:
            continue
        seen.add(code)
        conn.execute(
            "INSERT INTO stocks (symbol, code, name, is_active, last_trade_date, updated_at)"
            " VALUES (?, ?, ?, 0, ?, CURRENT_TIMESTAMP)",
            (symbol, code, name, last_date),
        )
        merged += 1
    conn.commit()
    if merged:
        logger.info(f"合并退市股清单: 新增 {merged} 只退市股 (is_active=0)")
    return True


def _sync_industries_via_xueqiu(conn):
    """雪球并发补全行业 (industry 为 NULL 的活跃股; 退市股雪球无资料, 不请求)

    东财批量接口 (push2) 已风控不可用, 行业改用雪球个股资料 (affiliate_industry.ind_name)。

    返回 (processed, failed): failed 仅计异常性失败; 雪球无行业资料属正常情况, 不计失败。
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT symbol, code FROM stocks WHERE industry IS NULL AND is_active = 1"
    )
    pending = cursor.fetchall()
    if not pending:
        logger.info("行业无需补全")
        return 0, 0

    detail_collector = StockDetailCollector()
    from utils.financial import get_market_label

    def fetch_industry(symbol, code):
        """并发 worker: 仅做网络抓取, 不触碰数据库连接。"""
        label = get_market_label(code).value.lower()
        info = detail_collector.fetch_from_xueqiu(f"{label}{code}")
        time.sleep(random.uniform(0.05, 0.1))
        return symbol, info.get("industry_xq")

    max_workers = 2
    commit_every = 20
    updated = 0
    failed = 0
    no_data = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(fetch_industry, s, c) for s, c in pending]
        for future in tqdm(as_completed(futures), total=len(futures), desc="补全行业"):
            try:
                symbol, industry = future.result()
            except Exception:
                failed += 1
                continue
            if not industry:
                no_data += 1
                continue
            cursor.execute(
                "UPDATE stocks SET industry = ?, updated_at = CURRENT_TIMESTAMP"
                " WHERE symbol = ?",
                (industry, symbol),
            )
            updated += 1
            if updated % commit_every == 0:
                conn.commit()
    conn.commit()
    logger.info(
        f"行业补全完成: 更新 {updated} 只, 失败 {failed} 只, 无资料 {no_data} 只"
    )
    return len(pending), failed


def sync_stock_metadata(run_industry=True, run_list_info=True):
    """补全股票元数据 (行业、上市日期等)

    返回 (processed, failed): failed 仅计异常性失败 (网络/解析错误);
    数据源无资料属正常情况 (已走巨潮/新浪 klc 兜底), 不计入失败。
    """
    conn = db_manager.get_sqlite_conn()
    total_processed = 0
    total_failed = 0
    if run_industry:
        logger.info("--- 正在批量同步行业信息 (雪球源) ---")
        processed, failed = _sync_industries_via_xueqiu(conn)
        total_processed += processed
        total_failed += failed

    if run_list_info:
        logger.info("--- 正在补全个股上市详情 (地域、日期, 雪球并发) ---")
        from storage.database.sync_status import (
            DATASET_STOCK_METADATA,
            record_sync_success,
        )

        cursor = conn.cursor()
        cursor.execute(
            "SELECT s.symbol, s.code FROM stocks s"
            " WHERE (s.area IS NULL OR TRIM(s.area) = ''"
            " OR s.list_date IS NULL OR TRIM(s.list_date) = '')"
        )
        pending = cursor.fetchall()
        if not pending:
            logger.info("个股详情无需补全")
            return total_processed, total_failed

        detail_collector = StockDetailCollector()
        from utils.financial import get_market_label

        def fetch_detail(symbol, code):
            """并发 worker: 仅做网络抓取, 不触碰数据库连接。

            巨潮兜底不得在 worker 内执行: akshare 的巨潮接口依赖 V8 引擎
            (py_mini_racer.MiniRacer), 其构造/使用非线程安全, 并发触发
            FATAL 崩溃 (Check failed: !pool->IsInitialized()), 故兜底统一
            放到主线程串行执行。
            """
            label = get_market_label(code).value.lower()
            info = detail_collector.fetch_from_xueqiu(f"{label}{code}")
            if not info.get("list_date"):
                info["list_date"] = detail_collector.fetch_from_eastmoney(code).get(
                    "list_date"
                )
            time.sleep(random.uniform(0.05, 0.1))
            return symbol, code, info

        # 串行请求是主要瓶颈 (网络延迟 ~0.7s/只), 采用 2 并发 + 主线程串行写库,
        # 兼顾速度与雪球风控; 每 COMMIT_EVERY 只提交一次, 中断不丢已写入数据。
        def normalize_metadata_value(value):
            if value is None:
                return None
            if isinstance(value, str):
                value = value.strip()
                return value or None
            try:
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                pass
            return value

        max_workers = 2
        commit_every = 20
        updated = 0
        failed = 0
        no_data = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(fetch_detail, s, c) for s, c in pending]
            for future in tqdm(
                as_completed(futures), total=len(futures), desc="补全详情"
            ):
                try:
                    symbol, code, info = future.result()
                except Exception:
                    failed += 1
                    continue
                if not info:
                    no_data += 1
                    continue
                if not info.get("list_date") or not info.get("area"):
                    # 雪球对部分股票 (境外注册/部分北交所) 返回空资料, 巨潮兜底
                    # (官方披露平台)。此处位于主线程, 避免 V8 引擎并发崩溃。
                    cninfo = detail_collector.fetch_from_cninfo(code)
                    info["area"] = info.get("area") or cninfo.get("area")
                    info["list_date"] = info.get("list_date") or cninfo.get("list_date")
                if not info.get("list_date"):
                    # 老退市股兜底: 雪球/东财/巨潮均无元数据, 用新浪 klc_kl.js
                    # 首条记录日期作为上市日 (V8 解密, 须主线程串行)。
                    from utils.sina_klc import SinaKlcFetcher, SinaKlcFetchError

                    try:
                        list_date = SinaKlcFetcher.fetch_list_date(code)
                    except SinaKlcFetchError as exc:
                        failed += 1
                        logger.warning(f"{symbol} 新浪 KLC 上市日期获取失败: {exc}")
                        continue
                    if list_date:
                        info["list_date"] = list_date
                area = normalize_metadata_value(info.get("area"))
                list_date = normalize_metadata_value(info.get("list_date"))
                industry = normalize_metadata_value(info.get("industry_xq"))
                metadata_complete = bool(area and list_date)
                if not metadata_complete:
                    no_data += 1
                cursor.execute(
                    "UPDATE stocks SET area = ?, list_date = ?,"
                    " industry = COALESCE(industry, ?), updated_at = CURRENT_TIMESTAMP"
                    " WHERE symbol = ?",
                    (
                        area,
                        list_date,
                        industry,
                        symbol,
                    ),
                )
                if metadata_complete:
                    record_sync_success(
                        DATASET_STOCK_METADATA, symbol, datetime.now().date()
                    )
                else:
                    logger.warning(
                        f"{symbol} 个股详情仍缺少地域或上市日期, 下次同步继续补全"
                    )
                updated += 1
                if updated % commit_every == 0:
                    conn.commit()
        conn.commit()
        logger.info(
            f"个股详情补全完成: 更新 {updated} 只, 失败 {failed} 只, 无资料 {no_data} 只"
        )
        total_processed += len(pending)
        total_failed += failed
    return total_processed, total_failed


def get_target_report_dates():
    today = datetime.now()
    dates = []
    for i in range(4):
        month = ((today.month - 1) // 3 - i) * 3
        curr_year = today.year
        while month <= 0:
            month += 12
            curr_year -= 1
        dates.append(f"{curr_year}{month:02d}{31 if month in [3, 12] else 30}")
    return dates


def _record_ttm_pending(symbol: str) -> None:
    """记录待重算 TTM 状态，供所有 TTM 调用路径复用。"""
    from storage.database.sync_status import (
        DATASET_FINANCIAL_TTM_PENDING,
        record_sync_success,
    )

    record_sync_success(DATASET_FINANCIAL_TTM_PENDING, symbol, datetime.now().date())


def _clear_ttm_pending(symbol: str) -> None:
    """清除已成功重算的 TTM 待处理状态。"""
    from storage.database.sync_status import (
        DATASET_FINANCIAL_TTM_PENDING,
        clear_sync_status,
    )

    clear_sync_status(DATASET_FINANCIAL_TTM_PENDING, symbol)


def _calculate_ttm_and_update_status(calculator, symbol: str) -> None:
    """计算单股 TTM，失败留痕，成功清除待重算状态。"""
    try:
        calculator.calculate_for_symbol(symbol)
        _clear_ttm_pending(symbol)
    except Exception:
        _record_ttm_pending(symbol)
        raise


def sync_financial_statements(symbol=None, force_all=False):
    """同步财务三大报表

    返回 (processed, failed): failed 为单股同步异常数 (网络/解析/存储错误,
    重试耗尽后计入); SinaBlockedError 不在此计数, 直接向上传播由调用方判定中止。
    """
    from data_ingestion.collectors.financial_collector import _SINA_NO_DATA_OVERRIDES
    from storage.database.sync_status import (
        DATASET_FINANCIAL_INCOMPLETE,
        DATASET_FINANCIAL_OFFICIAL_PENDING,
        DATASET_FINANCIAL_TTM_PENDING,
        clear_sync_status,
        get_last_sync_date,
        record_sync_success,
    )

    store = FinancialStore()
    collector = FinancialCollector()
    all_stocks = get_all_stocks()
    all_codes = [s[0] for s in all_stocks]
    target_codes = set()

    if symbol:
        target_codes = {symbol}
    elif force_all:
        logger.info("强制全量模式：扫描所有活跃股...")
        target_codes = set(all_codes)
    else:
        report_dates = get_target_report_dates()
        existing = store.get_existing_report_dates()
        for r_date in report_dates:
            df = collector.get_disclosure_plans(r_date)
            if not df.empty:
                df["actual_date"] = pd.to_datetime(df["actual_date"], errors="coerce")
                for code in df[df["actual_date"].notna()]["code"]:
                    if f"{code}_{r_date}" not in existing:
                        target_codes.add(code)
        # 孤儿补全排除已确认财务不完整 (确证缺表) 的股票, 避免每轮重复补全
        target_codes.update(
            c
            for c in get_orphan_codes("financial", all_codes)
            if get_last_sync_date(DATASET_FINANCIAL_INCOMPLETE, c) is None
        )
        target_codes.update(
            c
            for c in all_codes
            if get_last_sync_date(DATASET_FINANCIAL_OFFICIAL_PENDING, c) is not None
        )
        target_codes.update(
            c
            for c in all_codes
            if get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, c) is not None
        )

    if not target_codes:
        logger.info("财务报表数据已是最新。")
        return 0, 0

    symbol_name_map = {s[0]: s[1] for s in all_stocks}
    # 上市日期映射：用于官方核验时过滤上市前报告期，避免招股书回溯期误判超期
    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    list_date_rows = cursor.execute("SELECT symbol, list_date FROM stocks").fetchall()
    list_date_map = {
        symbol: list_date
        for symbol, list_date in list_date_rows
        if list_date and str(list_date).strip()
    }
    pbar = tqdm(list(target_codes), desc="报表同步")
    failed = 0
    official_date_resolver = OfficialDisclosureDateResolver()
    ttm_recalculator = None
    for code in pbar:
        name = symbol_name_map.get(code, "")
        pbar.set_description(f"报表同步: {code} {name}")
        ttm_recalculation_required = (
            get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, code) is not None
        )
        source_date_changes: dict[str, int] = {}
        try:
            stat_map = {
                "balance": "fin_balance_sheet",
                "profit": "fin_income_statement",
                "cashflow": "fin_cashflow_statement",
            }
            for st, table_name in stat_map.items():
                df = collector.fetch_statement(code, st)
                if not df.empty:
                    reconciliation_changes = store.save_statement(df, table_name) or {}
                    for source_name, changed_count in reconciliation_changes.items():
                        source_date_changes[source_name] = (
                            source_date_changes.get(source_name, 0) + changed_count
                        )
                time.sleep(random.uniform(3.0, 6.0))

            minimum_report_date = list_date_map.get(code)
            verification = verify_overdue_financial_publish_dates_for_symbol(
                code,
                resolver=official_date_resolver,
                minimum_report_date=minimum_report_date,
            )
            ttm_recalculation_required = ttm_recalculation_required or bool(
                source_date_changes or verification.changed_rows
            )
            if ttm_recalculation_required:
                if ttm_recalculator is None:
                    from analysis.processors.ttm_calculator import TTMCalculator

                    ttm_recalculator = TTMCalculator()
                _calculate_ttm_and_update_status(ttm_recalculator, code)
                logger.info("%s 公告日期修正后已重算 TTM", code)
            if verification.unresolved_report_dates:
                record_sync_success(
                    DATASET_FINANCIAL_OFFICIAL_PENDING,
                    code,
                    datetime.now().date(),
                )
                failed += 1
            else:
                clear_sync_status(DATASET_FINANCIAL_OFFICIAL_PENDING, code)
        except SinaBlockedError as e:
            logger.error(f"新浪接口 IP 风控，中止报表同步: {e}")
            raise
        except Exception:
            failed += 1
            if ttm_recalculation_required:
                _record_ttm_pending(code)
            record_sync_success(
                DATASET_FINANCIAL_OFFICIAL_PENDING,
                code,
                datetime.now().date(),
            )
            logger.exception(f"{code} 报表同步失败")
        # 该股存在确证缺表的报表类型 → 记录标记, 孤儿补全不再选中
        if any((code, st) in _SINA_NO_DATA_OVERRIDES for st in stat_map):
            record_sync_success(
                DATASET_FINANCIAL_INCOMPLETE, code, datetime.now().date()
            )
            logger.info(f"{code} 已确认财务数据不完整, 记录标记")

    return len(target_codes), failed


def sync_financial_indicators(symbol=None, force_all=False):
    """同步东财财务指标

    返回 (processed, failed): failed 为单股同步异常数 (网络/解析/存储错误)。
    """
    from storage.database.sync_status import (
        DATASET_FINANCIAL_DATE_RECONCILIATION_PENDING,
        DATASET_FINANCIAL_TTM_PENDING,
        clear_sync_status,
        get_last_sync_date,
        record_sync_success,
    )

    store = IndicatorStore()
    collector = FinancialCollector()
    all_stocks = get_all_stocks()
    target_tasks = []

    if symbol:
        target_tasks = [s for s in all_stocks if s[0] == symbol]
    elif force_all:
        target_tasks = all_stocks
    else:
        # ... 保持现有增量逻辑 ...
        report_dates = get_target_report_dates()
        existing = store.get_existing_report_dates()
        target_codes = set()
        for r_date in report_dates:
            df = collector.get_disclosure_plans(r_date)
            if not df.empty:
                df["actual_date"] = pd.to_datetime(df["actual_date"], errors="coerce")
                for code in df[df["actual_date"].notna()]["code"]:
                    if f"{code}_{r_date}" not in existing:
                        target_codes.add(code)
        target_codes.update(get_orphan_codes("indicators", [s[0] for s in all_stocks]))
        target_codes.update(
            s[0]
            for s in all_stocks
            if get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, s[0]) is not None
        )
        target_codes.update(
            s[0]
            for s in all_stocks
            if get_last_sync_date(DATASET_FINANCIAL_DATE_RECONCILIATION_PENDING, s[0])
            is not None
        )
        target_tasks = [s for s in all_stocks if s[0] in target_codes]

    if not target_tasks:
        logger.info("指标数据已是最新。")
        return 0, 0

    pbar = tqdm(target_tasks, desc="指标同步")
    from utils.financial import get_market_label

    failed = 0
    ttm_recalculator = None
    for code, name in pbar:
        pbar.set_description(f"指标同步: {code} {name}")
        ttm_recalculation_required = (
            get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, code) is not None
        )
        try:
            # 东财接口需要带后缀的代码 (如 600519.SH)
            label = get_market_label(code).value
            fmt_symbol = f"{code}.{label}"
            reconciliation_result = collector.collect_indicators(code, fmt_symbol)
            if reconciliation_result is not None:
                clear_sync_status(DATASET_FINANCIAL_DATE_RECONCILIATION_PENDING, code)
            reconciliation_changes = reconciliation_result or {}
            if reconciliation_changes or ttm_recalculation_required:
                if ttm_recalculator is None:
                    from analysis.processors.ttm_calculator import TTMCalculator

                    ttm_recalculator = TTMCalculator()
                _calculate_ttm_and_update_status(ttm_recalculator, code)
            time.sleep(random.uniform(0.5, 1.0))
        except Exception:
            failed += 1
            record_sync_success(
                DATASET_FINANCIAL_DATE_RECONCILIATION_PENDING,
                code,
                datetime.now().date(),
            )
            logger.exception(f"{code} 指标同步失败")

    return len(target_tasks), failed


def calculate_ttm_metrics(symbol=None, force_all=False):
    """
    计算滚动十二个月 (TTM) 财务指标。

    候选集构建原则 (孤儿股补全):
    - stocks 表 (sync-stocks 差量更新) 中已退市股以 is_active=0 标记, 但仍保留历史记录;
    - 退市股的历史财务报表保留在 Parquet 数据湖中 (孤儿股);
    - 因此 TTM 候选集必须以"数据湖实际存在的报表"为准 (fin_income_statement),
      而非仅 stocks 表, 否则孤儿股的 TTM 永远不会被批量重算。

    返回 (processed, failed): failed 为单股计算异常数。
    """
    from analysis.processors.ttm_calculator import TTMCalculator
    from storage.database.sync_status import (
        DATASET_FINANCIAL_TTM_PENDING,
        get_last_sync_date,
    )

    calculator = TTMCalculator()
    all_stocks = get_all_stocks()

    candidates = []  # 存储 (symbol, max_src_date)

    if symbol:
        # 单只模式: 不依赖 stocks 表, 直接按数据湖中是否存在报表判断
        if symbol not in get_financial_symbols():
            logger.warning(f"数据湖中无 {symbol} 的财务报表, 跳过")
            return 0, 0
        logger.info(f"单只同步模式: {symbol}")
        try:
            _calculate_ttm_and_update_status(calculator, symbol)
        except Exception:
            logger.exception(f"{symbol} TTM 计算失败")
            return 1, 1
        return 1, 0

    duckdb_conn = db_manager.get_duckdb_conn()
    db_manager.ensure_views("fin_income_statement", "fin_ttm")
    available_views = db_manager.list_available_views()
    financial_symbols = get_financial_symbols()
    pending_symbols = {
        code
        for code in financial_symbols
        if get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, code) is not None
    }

    if force_all:
        logger.info("强制全量模式...")
        # 候选集 = 数据湖中实际有报表的全部股票 (含孤儿股, 覆盖退市股),
        # 而非仅 stocks 活跃列表, 避免孤儿股 TTM 永不被重算
        candidates = [(s, "20991231") for s in financial_symbols]
    else:
        logger.info("智能增量模式：正在进行数据完整性自检...")
        if "fin_ttm" not in available_views:
            query = "SELECT symbol, MAX(report_date) FROM fin_income_statement GROUP BY symbol"
            candidates = duckdb_conn.execute(query).fetchall()
        else:
            sql = """
                SELECT src.symbol, src.max_src 
                FROM (SELECT symbol, MAX(report_date) as max_src FROM fin_income_statement GROUP BY symbol) src
                LEFT JOIN (SELECT symbol, MAX(report_date) as max_ttm FROM fin_ttm GROUP BY symbol) ttm
                  ON src.symbol = ttm.symbol
                WHERE ttm.max_ttm IS NULL OR src.max_src > ttm.max_ttm
            """
            candidates = duckdb_conn.execute(sql).fetchall()

    if not candidates and not pending_symbols:
        logger.info("所有 TTM 数据已是最新。")
        return 0, 0

    target_symbols = []
    selected_symbols = set()
    for code, max_date in candidates:
        if force_all:
            target_symbols.append(code)
            selected_symbols.add(code)
            continue
        if code in pending_symbols:
            target_symbols.append(code)
            selected_symbols.add(code)
            continue
        required_reports = get_consecutive_reports(max_date, 5)
        check_sql = f"SELECT COUNT(DISTINCT report_date) FROM fin_income_statement WHERE symbol = '{code}' AND report_date IN {tuple(required_reports)}"
        count = duckdb_conn.execute(check_sql).fetchone()[0]
        if count == 5:
            target_symbols.append(code)
            selected_symbols.add(code)
        else:
            logger.debug(
                f"跳过数据不全股票 {code}: 最新 {max_date} 往前 5 季仅有 {count} 季数据"
            )

    target_symbols.extend(sorted(pending_symbols - selected_symbols))

    if not target_symbols:
        logger.info("数据完整性未达标，无需计算。")
        return 0, 0

    symbol_name_map = {s[0]: s[1] for s in all_stocks}
    logger.info(f"开始为 {len(target_symbols)} 只股票同步 TTM 指标...")
    pbar = tqdm(target_symbols, desc="TTM 计算")
    failed = 0
    for code in pbar:
        name = symbol_name_map.get(code, "")
        pbar.set_description(f"TTM 计算: {code} {name}")
        try:
            _calculate_ttm_and_update_status(calculator, code)
        except Exception:
            failed += 1
            logger.exception(f"{code} TTM 计算失败")

    return len(target_symbols), failed


def sync_share_capital(symbol=None, force_all=False, start_date=None):
    """同步股本变动记录

    返回 (processed, failed): failed 为单股同步异常数 (重试耗尽后计入)。
    """
    from data_ingestion.collectors.share_collector import ShareCollector
    from storage.database.sync_status import (
        DATASET_SHARE_CAPITAL,
        is_synced_today,
    )
    from utils.trade_date import get_latest_trade_date

    collector = ShareCollector()
    all_stocks = get_all_stocks()
    target_tasks = [s for s in all_stocks if s[0] == symbol] if symbol else all_stocks

    if force_all and not start_date:
        start_date = "19900101"
        logger.info(f"强制全量模式：将从 {start_date} 开始同步股本变动")

    latest_trade_date = get_latest_trade_date().strftime("%Y%m%d")
    logger.info(
        f"开始同步 {len(target_tasks)} 只股票的股本变动 (基准日期: {latest_trade_date})..."
    )
    skipped = 0
    failed = 0
    pbar = tqdm(target_tasks, desc="股本同步")
    for code, name in pbar:
        pbar.set_description(f"股本同步: {code} {name}")
        # 单股/强制模式绕过"当日已同步"检查, 默认模式跳过今日已同步股票
        if (
            not symbol
            and not force_all
            and is_synced_today(DATASET_SHARE_CAPITAL, code)
        ):
            skipped += 1
            continue
        try:
            collector.collect_share_capital(code, start_date=start_date)
        except SinaBlockedError:
            # 新浪 IP 风控: 重试无意义, 立即中止整个流水线 (由调用方判定 BLOCKED)
            raise
        except Exception:
            failed += 1
            logger.error(f"{code} {name} 股本同步最终失败 (已重试)")
        time.sleep(random.uniform(1, 1.5))

    logger.info(
        f"股本同步完成: 共 {len(target_tasks)} 只, 本次同步 {len(target_tasks) - skipped - failed} 只, "
        f"跳过 {skipped} 只 (今日已同步), 失败 {failed} 只"
    )
    return len(target_tasks), failed


def sync_daily_kline(
    symbol=None,
    force_all=False,
    start_date=None,
):
    with CanonicalWriteLock(operation="sync-kline", run_id=symbol or "ALL"):
        return _sync_daily_kline(
            symbol=symbol,
            force_all=force_all,
            start_date=start_date,
        )


def _sync_daily_kline(symbol=None, force_all=False, start_date=None):
    """同步日线行情数据 (新浪源, 串行 + 保守节奏)

    返回 (processed, failed): failed 为单股同步异常数 (重试耗尽后计入)。
    """
    from data_ingestion.collectors.kline_collector import DailyKlineCollector
    from utils.trade_date import get_latest_trade_date

    collector = DailyKlineCollector(source="sina-klc")
    all_stocks = get_all_stocks()
    target_tasks = [s for s in all_stocks if s[0] == symbol] if symbol else all_stocks

    if force_all and not start_date:
        start_date = MIN_KLINE_START_DATE
        logger.info(f"强制全量模式：将从 {start_date} 开始同步 K 线数据")

    latest_date = get_latest_trade_date().strftime("%Y%m%d")
    logger.info(
        f"开始同步 {len(target_tasks)} 只股票的日线行情 (基准日期: {latest_date})..."
    )
    skipped = 0
    failed = 0
    pbar = tqdm(target_tasks, desc="K线同步")
    for code, name in pbar:
        pbar.set_description(f"K线同步: {code} {name}")
        try:
            synced = collector.collect_kline(
                code, start_date=start_date, end_date=latest_date
            )
            if synced:
                time.sleep(random.uniform(2.0, 4.0))
            else:
                skipped += 1
        except SinaBlockedError:
            # 新浪 IP 风控: 重试无意义, 立即中止整个流水线 (由调用方判定 BLOCKED)
            raise
        except Exception:
            failed += 1
            logger.error(f"{code} {name} K线同步最终失败 (已重试)")
    logger.info(
        f"K线同步完成 (本次同步 {len(target_tasks) - skipped - failed} 只, "
        f"跳过 {skipped} 只已为最新, 失败 {failed} 只)"
    )
    return len(target_tasks), failed


def sync_etf_list():
    """同步ETF基础列表 (etfs 表)"""
    from data_ingestion.collectors.etf_collector import ETFListCollector

    collector = ETFListCollector()
    df = collector.fetch_all_etfs()
    if df.empty:
        return
    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM etfs")
    df.to_sql("etfs", conn, if_exists="append", index=False)
    conn.commit()
    logger.info(f"成功同步 {len(df)} 条记录到 etfs 表")


def get_active_etfs():
    """获取所有活跃ETF的 (code, name)"""
    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT code, name FROM etfs WHERE is_active = 1")
    return cursor.fetchall()


def sync_etf_kline(symbol=None, force_all=False, start_date=None):
    """同步ETF日线行情数据"""
    from data_ingestion.collectors.etf_collector import ETFKlineCollector
    from utils.trade_date import get_latest_trade_date

    collector = ETFKlineCollector()
    all_active = get_active_etfs()
    target_tasks = [s for s in all_active if s[0] == symbol] if symbol else all_active

    if force_all and not start_date:
        start_date = "19900101"
        logger.info(f"强制全量模式：将从 {start_date} 开始同步 ETF K 线数据")

    latest_date = get_latest_trade_date().strftime("%Y%m%d")
    logger.info(
        f"开始同步 {len(target_tasks)} 只ETF的日线行情 (基准日期: {latest_date})..."
    )
    pbar = tqdm(target_tasks, desc="ETF K线同步")
    for code, name in pbar:
        pbar.set_description(f"ETF K线同步: {code} {name}")
        try:
            collector.collect_kline(code, start_date=start_date)
        except Exception:
            logger.error(f"{code} {name} ETF K线同步最终失败 (已重试)")
        time.sleep(random.uniform(0.5, 1.0))


def sync_all_data_flow(symbol=None, force_all=False) -> str:
    """执行全量数据同步流水线 (含名单/元数据; 单股模式跳过名单与元数据)

    返回三态:
    - SYNC_ALL_SUCCESS: 全部 7 环节失败计数均为 0
    - SYNC_ALL_RETRYABLE: 存在环节失败 (可整体重试, 增量机制自动跳过已完成部分)
    - SYNC_ALL_BLOCKED: 新浪 IP 风控中止 (重试无意义, 需等待解封)
    """
    logger.info(">>> 开始执行一键数据同步流水线 <<<")
    stage_stats: dict[str, tuple[int, int]] = {}
    try:
        if not symbol:
            # 名单与元数据是全市场操作, 单股模式跳过
            stage_stats["stocks"] = sync_stock_list()
            stage_stats["metadata"] = sync_stock_metadata()
        # 先同步指标，因为指标表（东财源）的公告日期和更新日期更准确，用于后续修复三张表
        stage_stats["indicators"] = sync_financial_indicators(
            symbol=symbol, force_all=force_all
        )
        stage_stats["financial"] = sync_financial_statements(
            symbol=symbol, force_all=force_all
        )
        stage_stats["ttm"] = calculate_ttm_metrics(symbol=symbol, force_all=force_all)
        stage_stats["share"] = sync_share_capital(symbol=symbol, force_all=force_all)
        stage_stats["kline"] = sync_daily_kline(symbol=symbol, force_all=force_all)
    except SinaBlockedError as e:
        logger.error(f">>> 新浪接口 IP 风控，数据同步流水线中止 (已同步数据保留): {e}")
        logger.error(
            ">>> 请等待 5~60 分钟封禁解除后重试 (增量同步会自动跳过已完成部分) <<<"
        )
        return SYNC_ALL_BLOCKED

    total_failed = 0
    for name, (processed, failed) in stage_stats.items():
        logger.info(f"流水线环节 {name}: 处理 {processed} 项, 失败 {failed} 项")
        total_failed += failed
    if total_failed > 0:
        logger.error(
            f">>> 数据同步流水线存在失败: 合计失败 {total_failed} 项, 判定为可重试 <<<"
        )
        return SYNC_ALL_RETRYABLE
    logger.info(">>> 数据同步流水线执行完成 (全部环节成功) <<<")
    return SYNC_ALL_SUCCESS


def export_duckdb_views(output_path: str):
    """导出 DuckDB 视图的 SQL 定义"""
    sql = db_manager.generate_full_sql()
    Path(output_path).write_text(sql, encoding="utf-8")
    logger.info(f"视图脚本已导出至: {output_path}")


def rebuild_view_schemas(dataset: str = None):
    """重建视图 schema 预声明缓存 (字段变更后必须执行)"""
    from storage.database.schema_builder import (
        DATASET_PATTERNS,
        rebuild_all,
        rebuild_dataset,
    )

    if dataset:
        if dataset not in DATASET_PATTERNS:
            logger.error(f"未知数据集: {dataset} (可选: {', '.join(DATASET_PATTERNS)})")
            return
        rebuild_dataset(dataset)
        logger.info(f"schema 缓存重建完成: {dataset}")
    else:
        rebuild_all()
        logger.info("全部 schema 缓存重建完成")


def run_backtest(backtest_config_path: str):
    """按 TOML 配置运行已注册策略并输出可复现的研究产物。"""
    from backtest.config import load_backtest_config
    from backtest.reporter import write_backtest_result
    from backtest.runner import execute_backtest

    config = load_backtest_config(backtest_config_path)
    backtest_run = execute_backtest(config, db_manager)
    output_dir = write_backtest_result(
        backtest_run.config,
        backtest_run.result.daily_nav,
        backtest_run.targets,
        backtest_run.result.trades,
    )
    logger.info(f"回测完成，结果目录: {output_dir}")


def evaluate_factor_experiments(
    research_config_path: str,
    output_path: str | None = None,
):
    """按固定切分和 Walk-forward 评估候选因子实验。"""
    from backtest.research_evaluator import (
        evaluate_factor_experiments as evaluate_experiments,
    )
    from backtest.research_evaluator import (
        load_factor_experiment_evaluation_config,
        write_factor_experiment_evaluation_report,
    )

    config = load_factor_experiment_evaluation_config(research_config_path)
    result = evaluate_experiments(config, db_manager)
    if output_path:
        output_dir = Path(output_path)
    else:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("workspace/backtest/evaluations") / (
            f"{config.name}_{run_id}"
        )
    output_dir = write_factor_experiment_evaluation_report(result, output_dir)
    logger.info(f"因子实验评估完成，结果目录: {output_dir}")
    return output_dir


def run_factor_diagnostics(
    factor_names: list[str],
    start_date: str,
    end_date: str,
    horizons: list[int],
    quantile_count: int,
    output_path: str | None = None,
):
    """Run point-in-time diagnostics for registered factors."""
    from datetime import date, timedelta

    from analysis.factors.diagnostics import (
        calculate_factor_diagnostics,
        write_factor_diagnostic_report,
    )
    from analysis.factors.engine import FactorEngine
    from backtest.config import BacktestConfig
    from backtest.data_access import BacktestDataAccess

    try:
        signal_start_date = date.fromisoformat(start_date)
        signal_end_date = date.fromisoformat(end_date)
    except ValueError as error:
        raise ValueError("因子诊断日期必须使用 YYYY-MM-DD 格式") from error
    if signal_start_date >= signal_end_date:
        raise ValueError("因子诊断开始日期必须早于结束日期")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("因子诊断持有期必须是正整数")

    # 预留足够的自然日，覆盖节假日后仍能取得最长持有期的收盘价。
    data_end_date = signal_end_date + timedelta(days=max(horizons) * 3 + 30)
    config = BacktestConfig(
        start_date=signal_start_date,
        end_date=signal_end_date,
        strategy_name="factor-diagnostics",
        benchmark_symbol=None,
    )
    data_access = BacktestDataAccess(db_manager)
    input_data = data_access.load_factor_data(
        config,
        tuple(factor_names),
        data_end_date=data_end_date,
    )
    factor_frame = FactorEngine().calculate(input_data, tuple(factor_names))
    price_frame = input_data.loc[:, ["date", "symbol", "open_hfq", "close_hfq"]]
    diagnostic_input = price_frame.merge(
        factor_frame,
        on=["date", "symbol"],
        how="inner",
        validate="one_to_one",
    )
    report = calculate_factor_diagnostics(
        diagnostic_input,
        tuple(factor_names),
        horizons=tuple(horizons),
        quantile_count=quantile_count,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
    )
    if output_path:
        output_dir = Path(output_path)
    else:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("workspace/factor_diagnostics") / f"run_{run_id}"
    output_dir = write_factor_diagnostic_report(
        report,
        output_dir,
        parameters={
            "factor_names": factor_names,
            "signal_start_date": signal_start_date.isoformat(),
            "signal_end_date": signal_end_date.isoformat(),
            "data_end_date": data_end_date.isoformat(),
            "horizons": sorted(set(horizons)),
            "quantile_count": quantile_count,
        },
    )
    logger.info(f"因子诊断完成，结果目录: {output_dir}")
    return output_dir


def list_registered_backtest_strategies():
    """列出策略注册表，避免用户依赖代码文件名猜测策略名称。"""
    from backtest.strategy_registry import list_backtest_strategies

    for metadata in list_backtest_strategies():
        print(f"{metadata.name} (v{metadata.version}): {metadata.description}")
        print(f"  参数: {metadata.parameter_summary}")


# --- CLI 定义 ---


def main():
    from utils.requests_protection import install_requests_protection

    install_requests_protection()

    parser = argparse.ArgumentParser(
        description="QuantPyLab 实验室统一入口",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="子命令集")

    # 1. sync-stocks
    subparsers.add_parser("sync-stocks", help="同步 A 股全量股票代码与名称 (stocks表)")

    # 2. sync-metadata
    meta_p = subparsers.add_parser(
        "sync-metadata", help="同步股票行业、地域、上市日期等元数据"
    )
    meta_p.add_argument("--industry", action="store_true", help="仅同步行业")
    meta_p.add_argument("--list-info", action="store_true", help="仅同步上市详情")

    # 3. sync-financial
    fin_p = subparsers.add_parser("sync-financial", help="同步历史财务报表")
    fin_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    fin_p.add_argument("--force-all", action="store_true", help="全量扫描所有股票")

    # 4. sync-indicators
    ind_p = subparsers.add_parser(
        "sync-indicators", help="同步东方财富 140+ 项财务指标"
    )
    ind_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    ind_p.add_argument("--force-all", action="store_true", help="全量扫描所有股票")

    # 5. calc-ttm
    ttm_p = subparsers.add_parser("calc-ttm", help="计算滚动财务 (TTM) 指标")
    ttm_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    ttm_p.add_argument("--force-all", action="store_true", help="全量重新计算所有股票")

    # 6. sync-share
    share_p = subparsers.add_parser("sync-share", help="同步股本变动记录")
    share_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    share_p.add_argument("--start-date", type=str, help="手动指定起始日期 (YYYYMMDD)")
    share_p.add_argument("--force-all", action="store_true", help="扫描所有股票")

    # 7. sync-kline
    kline_p = subparsers.add_parser("sync-kline", help="同步日线行情数据")
    kline_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    kline_p.add_argument("--start-date", type=str, help="手动指定起始日期 (YYYYMMDD)")
    kline_p.add_argument("--force-all", action="store_true", help="扫描所有股票")

    # 8. sync-etf-list
    subparsers.add_parser("sync-etf-list", help="同步场内交易基金列表 (etfs表)")

    # 9. sync-etf-kline
    etf_kline_p = subparsers.add_parser("sync-etf-kline", help="同步ETF日线行情数据")
    etf_kline_p.add_argument("--symbol", type=str, help="指定单只ETF代码")
    etf_kline_p.add_argument(
        "--start-date", type=str, help="手动指定起始日期 (YYYYMMDD)"
    )
    etf_kline_p.add_argument("--force-all", action="store_true", help="扫描所有活跃ETF")

    # 10. sync-all
    all_p = subparsers.add_parser(
        "sync-all", help="一键同步全流程数据 (含名单与元数据)"
    )
    all_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    all_p.add_argument("--force-all", action="store_true", help="全量强制同步")

    # 11. export-views
    exp_p = subparsers.add_parser(
        "export-views", help="导出 DuckDB 视图的 SQL 定义脚本"
    )
    exp_p.add_argument(
        "--output",
        "-o",
        type=str,
        default="docs/view_definition.sql",
        help="输出文件路径 (默认: docs/view_definition.sql)",
    )

    # 12. show-views
    subparsers.add_parser("show-views", help="显示视图依赖关系图 (PlantUML)")

    # 13. export-report
    rep_p = subparsers.add_parser("export-report", help="合并并导出公司深度研报为 PDF")
    rep_p.add_argument("--name", required=True, help="公司名称 (对应目录名)")
    rep_p.add_argument("--output", "-o", type=str, help="输出文件路径")

    # 14. rebuild-schemas
    rs_p = subparsers.add_parser(
        "rebuild-schemas", help="重建视图 schema 预声明缓存 (财务字段变更后必须执行)"
    )
    rs_p.add_argument("--dataset", "-d", type=str, help="仅重建指定数据集")

    # 15. run-backtest
    backtest_p = subparsers.add_parser("run-backtest", help="运行日频股票策略回测")
    backtest_p.add_argument(
        "--backtest-config", required=True, help="回测 TOML 配置文件路径"
    )

    # 16. list-backtest-strategies
    subparsers.add_parser("list-backtest-strategies", help="列出已注册的日频回测策略")

    # 17. diagnose-factors
    diagnostic_p = subparsers.add_parser(
        "diagnose-factors", help="运行点时因子覆盖率、IC 与稳定性诊断"
    )
    diagnostic_p.add_argument(
        "--factor-names",
        nargs="+",
        required=True,
        help="一个或多个已注册因子名称",
    )
    diagnostic_p.add_argument(
        "--start-date", required=True, help="信号起始日期 (YYYY-MM-DD)"
    )
    diagnostic_p.add_argument(
        "--end-date", required=True, help="信号结束日期 (YYYY-MM-DD)"
    )
    diagnostic_p.add_argument(
        "--horizons",
        nargs="+",
        type=int,
        default=[1, 5, 20],
        help="持有期交易日数量，默认 1 5 20",
    )
    diagnostic_p.add_argument(
        "--quantile-count",
        type=int,
        default=5,
        help="横截面分位数组数，默认 5",
    )
    diagnostic_p.add_argument(
        "--output", help="结果目录，默认写入 workspace/factor_diagnostics"
    )

    # 18. evaluate-factor-experiments
    evaluation_p = subparsers.add_parser(
        "evaluate-factor-experiments",
        help="按训练/验证/测试和 Walk-forward 评估候选因子实验",
    )
    evaluation_p.add_argument(
        "--research-config", required=True, help="研究评估 TOML 配置文件路径"
    )
    evaluation_p.add_argument(
        "--output", help="结果目录，默认写入 workspace/backtest/evaluations"
    )

    # 19. migrate-kline-source
    migration_p = subparsers.add_parser(
        "migrate-kline-source", help="分阶段重建全部股票日线数据源"
    )
    migration_p.add_argument(
        "--source", required=True, choices=["sina-klc"], help="明确指定迁移目标源"
    )
    migration_p.add_argument("--symbol", type=str, help="仅处理指定股票")
    migration_p.add_argument(
        "--limit",
        type=int,
        default=20,
        help="staging 默认处理数量，默认 20；使用 --all-stocks 取消限制",
    )
    migration_p.add_argument(
        "--all-stocks", action="store_true", help="选择全部股票（含退市股）"
    )
    migration_p.add_argument(
        "--start-date",
        default=MIN_KLINE_START_DATE,
        help=f"起始日期 (YYYYMMDD，最早 {MIN_KLINE_START_DATE})",
    )
    migration_p.add_argument("--end-date", help="结束日期 (YYYYMMDD)，默认最近交易日")
    migration_p.add_argument(
        "--stage-only",
        action="store_true",
        help="仅写入 staging，不替换 canonical；本阶段为唯一模式",
    )
    migration_p.add_argument(
        "--dry-run", action="store_true", help="只生成目标清单，不请求网络"
    )
    migration_p.add_argument("--resume", help="恢复指定 RUN_ID 的未完成迁移")
    migration_p.add_argument(
        "--recover-stale-lock",
        action="store_true",
        help="仅当锁内 PID 已退出时恢复遗留迁移锁",
    )

    # 18. promote-kline-staging
    promotion_p = subparsers.add_parser(
        "promote-kline-staging", help="将已验收 K 线 staging 晋级到 canonical"
    )
    promotion_p.add_argument(
        "--run-id", required=True, help="staging 或 promotion RUN_ID"
    )
    promotion_p.add_argument("--symbol", help="仅晋级指定股票，用于灰度验证")
    promotion_p.add_argument(
        "--dry-run", action="store_true", help="只校验 staging，不写 canonical"
    )
    promotion_p.add_argument(
        "--resume", action="store_true", help="恢复 promotion RUN_ID"
    )
    promotion_p.add_argument(
        "--recover-stale-lock",
        action="store_true",
        help="仅当 staging 锁内 PID 已退出时恢复遗留迁移锁",
    )

    # 19. rollback-kline-promotion
    rollback_p = subparsers.add_parser(
        "rollback-kline-promotion", help="回滚指定 K 线 canonical 晋级"
    )
    rollback_p.add_argument("--run-id", required=True, help="promotion RUN_ID")

    args = parser.parse_args()

    if args.command == "sync-stocks":
        sync_stock_list()
    elif args.command == "sync-metadata":
        sync_stock_metadata(
            run_industry=not args.list_info, run_list_info=not args.industry
        )
    elif args.command == "sync-financial":
        sync_financial_statements(symbol=args.symbol, force_all=args.force_all)
    elif args.command == "sync-indicators":
        sync_financial_indicators(symbol=args.symbol, force_all=args.force_all)
    elif args.command == "calc-ttm":
        calculate_ttm_metrics(symbol=args.symbol, force_all=args.force_all)
    elif args.command == "sync-share":
        sync_share_capital(
            symbol=args.symbol, force_all=args.force_all, start_date=args.start_date
        )
    elif args.command == "sync-kline":
        try:
            _processed, failed = sync_daily_kline(
                symbol=args.symbol, force_all=args.force_all, start_date=args.start_date
            )
        except Exception:
            logger.exception("日线同步异常退出")
            sys.exit(1)
        if failed:
            sys.exit(1)
    elif args.command == "sync-etf-list":
        sync_etf_list()
    elif args.command == "sync-etf-kline":
        sync_etf_kline(
            symbol=args.symbol, force_all=args.force_all, start_date=args.start_date
        )
    elif args.command == "sync-all":
        try:
            status = sync_all_data_flow(symbol=args.symbol, force_all=args.force_all)
        except Exception:
            logger.exception("sync-all 流水线异常中止, 退出码 1")
            sys.exit(1)
        if status == SYNC_ALL_BLOCKED:
            logger.error("sync-all 因新浪 IP 风控中止 (未全部成功), 退出码 1")
            sys.exit(1)
        elif status == SYNC_ALL_RETRYABLE:
            logger.warning("sync-all 存在环节失败, 可稍后重跑 (增量同步会自动补缺)")
            sys.exit(1)
    elif args.command == "export-views":
        export_duckdb_views(args.output)
    elif args.command == "show-views":
        puml = db_manager.get_view_relationships_puml()
        print("\n--- PlantUML Source ---")
        print(puml)
        print(
            "\n(You can copy this source to https://www.plantuml.com/plantuml/ to view the graph)\n"
        )
    elif args.command == "export-report":
        from utils.report_exporter import ReportExporter

        exporter = ReportExporter(args.name)
        out = exporter.export(args.output)
        logger.info(f"✅ 成功导出研报至: {out}")
    elif args.command == "rebuild-schemas":
        rebuild_view_schemas(dataset=args.dataset)
    elif args.command == "run-backtest":
        run_backtest(backtest_config_path=args.backtest_config)
    elif args.command == "list-backtest-strategies":
        list_registered_backtest_strategies()
    elif args.command == "diagnose-factors":
        try:
            run_factor_diagnostics(
                factor_names=args.factor_names,
                start_date=args.start_date,
                end_date=args.end_date,
                horizons=args.horizons,
                quantile_count=args.quantile_count,
                output_path=args.output,
            )
        except Exception:
            logger.exception("因子诊断异常退出")
            sys.exit(1)
    elif args.command == "evaluate-factor-experiments":
        try:
            evaluate_factor_experiments(
                research_config_path=args.research_config,
                output_path=args.output,
            )
        except Exception:
            logger.exception("因子实验评估异常退出")
            sys.exit(1)
    elif args.command == "migrate-kline-source":
        from tools.kline_source_migration import run_kline_source_migration

        if args.all_stocks and args.symbol:
            parser.error("--all-stocks 与 --symbol 不能同时使用")
        if args.resume and (args.symbol or args.limit != 20 or args.all_stocks):
            parser.error("--resume 不能同时指定 symbol 或 limit")
        if not args.stage_only:
            parser.error("当前迁移仅支持 --stage-only staging 模式")

        try:
            result = run_kline_source_migration(
                source=args.source,
                symbol=args.symbol,
                limit=None if args.all_stocks else args.limit,
                start_date=args.start_date,
                end_date=args.end_date,
                dry_run=args.dry_run,
                stage_only=args.stage_only,
                resume_run_id=args.resume,
                recover_stale_lock=args.recover_stale_lock,
            )
        except Exception:
            logger.exception("K 线数据源迁移异常退出")
            sys.exit(1)
        logger.info(
            "K 线数据源 staging 完成: run_id=%s staged=%d failed=%d",
            result.run_id,
            len(result.staged_symbols),
            len(result.failed_symbols),
        )
        print(f"run_id={result.run_id}")
        print(f"run_dir={result.run_dir}")
        if result.failed_symbols or result.stopped_by_sina_block:
            sys.exit(1)
    elif args.command == "promote-kline-staging":
        from tools.kline_promotion import (
            promote_kline_staging,
            resume_kline_promotion,
        )

        if args.resume and (args.symbol or args.dry_run):
            parser.error("--resume 不能同时指定 --symbol 或 --dry-run")
        try:
            if args.resume:
                result = resume_kline_promotion(
                    args.run_id,
                    recover_stale_lock=args.recover_stale_lock,
                )
            else:
                result = promote_kline_staging(
                    args.run_id,
                    symbol=args.symbol,
                    dry_run=args.dry_run,
                    recover_stale_lock=args.recover_stale_lock,
                )
        except Exception:
            logger.exception("K 线 staging 晋级异常退出")
            sys.exit(1)
        logger.info(
            "K 线 staging 晋级完成: promotion_run_id=%s status=%s promoted=%d validated=%d failed=%d dry_run=%s",
            result.promotion_run_id or "(dry-run)",
            result.status,
            len(result.promoted_symbols),
            len(result.validated_symbols),
            len(result.failed_symbols),
            result.dry_run,
        )
        print(f"promotion_run_id={result.promotion_run_id}")
        print(f"staging_run_id={result.staging_run_id}")
        if result.failed_symbols or result.status not in {
            "completed",
            "partial",
            "validated",
        }:
            sys.exit(1)
    elif args.command == "rollback-kline-promotion":
        from tools.kline_promotion import rollback_kline_promotion

        try:
            result = rollback_kline_promotion(args.run_id)
        except Exception:
            logger.exception("K 线 canonical 回滚异常退出")
            sys.exit(1)
        logger.info(
            "K 线 canonical 回滚完成: promotion_run_id=%s",
            result.promotion_run_id,
        )
        print(f"promotion_run_id={result.promotion_run_id}")
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("主程序执行异常退出")
    finally:
        db_manager.close_all()
