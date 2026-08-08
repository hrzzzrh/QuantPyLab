import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from data_ingestion.collectors.financial_collector import FinancialCollector
from data_ingestion.collectors.industry_collector import HighSpeedIndustryCollector
from data_ingestion.collectors.stock_list import (
    StockDetailCollector,
    StockListCollector,
)
from storage.database.financial_store import FinancialStore
from storage.database.indicator_store import IndicatorStore
from storage.database.manager import db_manager
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


def sync_stock_list():
    """同步股票列表: 差量 diff 更新 (新增插入, 存量更新, 消失标记退市)

    以 AkShare 当前在市列表为基准:
    - 新列表有、库中无   -> INSERT (is_active=1)
    - 两边都有          -> 更新 name (若曾被误标退市则恢复)
    - 库中有、新列表无   -> UPDATE is_active=0 (last_trade_date 保持 NULL,
      由退市股 K 线腾讯重建流程写入真实最后交易日)
    接口返回空列表时跳过, 防止数据源异常导致全库误标退市。
    """
    collector = StockListCollector()
    df = collector.fetch_all_stocks()
    if df.empty:
        logger.warning("股票列表接口返回为空, 跳过 diff 更新 (防止误标退市)")
        return

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
    logger.info(
        f"stocks 表 diff 同步完成: 在市 {len(incoming)} 只, 新增 {len(new_symbols)},"
        f" 退市 {delisted}, 库内总数 {total}"
    )


def sync_stock_metadata(run_industry=True, run_list_info=True):
    """补全股票元数据 (行业、上市日期等)"""
    conn = db_manager.get_sqlite_conn()
    if run_industry:
        logger.info("--- 正在批量同步行业信息 ---")
        HighSpeedIndustryCollector().sync_industries(conn)

    if run_list_info:
        logger.info("--- 正在补全个股上市详情 (地域、日期, 雪球并发) ---")
        cursor = conn.cursor()
        cursor.execute(
            "SELECT symbol, code FROM stocks WHERE area IS NULL OR list_date IS NULL"
        )
        pending = cursor.fetchall()
        if not pending:
            logger.info("个股详情无需补全")
            return

        detail_collector = StockDetailCollector()
        from utils.financial import get_market_label

        def fetch_detail(symbol, code):
            """并发 worker: 仅做网络抓取, 不触碰数据库连接。"""
            label = get_market_label(code).value.lower()
            info = detail_collector.fetch_from_xueqiu(f"{label}{code}")
            if not info.get("list_date"):
                info["list_date"] = detail_collector.fetch_from_eastmoney(code).get(
                    "list_date"
                )
            if not info.get("list_date") or not info.get("area"):
                # 雪球对部分股票 (境外注册/部分北交所) 返回空资料, 巨潮兜底 (官方披露平台)
                cninfo = detail_collector.fetch_from_cninfo(code)
                info["area"] = info.get("area") or cninfo.get("area")
                info["list_date"] = info.get("list_date") or cninfo.get("list_date")
            time.sleep(random.uniform(0.05, 0.1))
            return symbol, info

        # 串行请求是主要瓶颈 (网络延迟 ~0.7s/只), 采用 2 并发 + 主线程串行写库,
        # 兼顾速度与雪球风控; 每 COMMIT_EVERY 只提交一次, 中断不丢已写入数据。
        max_workers = 2
        commit_every = 20
        updated = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(fetch_detail, s, c) for s, c in pending]
            for future in tqdm(
                as_completed(futures), total=len(futures), desc="补全详情"
            ):
                try:
                    symbol, info = future.result()
                except Exception:
                    failed += 1
                    continue
                if not info:
                    failed += 1
                    continue
                cursor.execute(
                    "UPDATE stocks SET area = ?, list_date = ?, updated_at = CURRENT_TIMESTAMP WHERE symbol = ?",
                    (info.get("area"), info.get("list_date"), symbol),
                )
                updated += 1
                if updated % commit_every == 0:
                    conn.commit()
        conn.commit()
        logger.info(f"个股详情补全完成: 更新 {updated} 只, 失败 {failed} 只")


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


def sync_financial_statements(symbol=None, force_all=False):
    """同步财务三大报表"""
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
        target_codes.update(get_orphan_codes("financial", all_codes))

    if not target_codes:
        logger.info("财务报表数据已是最新。")
        return

    symbol_name_map = {s[0]: s[1] for s in all_stocks}
    pbar = tqdm(list(target_codes), desc="报表同步")
    for code in pbar:
        name = symbol_name_map.get(code, "")
        pbar.set_description(f"报表同步: {code} {name}")
        try:
            stat_map = {
                "balance": "fin_balance_sheet",
                "profit": "fin_income_statement",
                "cashflow": "fin_cashflow_statement",
            }
            for st, table_name in stat_map.items():
                df = collector.fetch_statement(code, st)
                if not df.empty:
                    store.save_statement(df, table_name)
                time.sleep(random.uniform(3.0, 6.0))
        except SinaBlockedError as e:
            logger.error(f"新浪接口 IP 风控，中止报表同步: {e}")
            raise
        except Exception:
            logger.exception(f"{code} 报表同步失败")


def sync_financial_indicators(symbol=None, force_all=False):
    """同步东财财务指标"""
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
        target_tasks = [s for s in all_stocks if s[0] in target_codes]

    if not target_tasks:
        logger.info("指标数据已是最新。")
        return

    pbar = tqdm(target_tasks, desc="指标同步")
    from utils.financial import get_market_label

    for code, name in pbar:
        pbar.set_description(f"指标同步: {code} {name}")
        try:
            # 东财接口需要带后缀的代码 (如 600519.SH)
            label = get_market_label(code).value
            fmt_symbol = f"{code}.{label}"
            collector.collect_indicators(code, fmt_symbol)
            time.sleep(random.uniform(0.5, 1.0))
        except Exception:
            logger.exception(f"{code} 指标同步失败")


def calculate_ttm_metrics(symbol=None, force_all=False):
    """
    计算滚动十二个月 (TTM) 财务指标。

    候选集构建原则 (孤儿股补全):
    - stocks 表 (sync-stocks 差量更新) 中已退市股以 is_active=0 标记, 但仍保留历史记录;
    - 退市股的历史财务报表保留在 Parquet 数据湖中 (孤儿股);
    - 因此 TTM 候选集必须以"数据湖实际存在的报表"为准 (fin_income_statement),
      而非仅 stocks 表, 否则孤儿股的 TTM 永远不会被批量重算。
    """
    from analysis.processors.ttm_calculator import TTMCalculator

    calculator = TTMCalculator()
    all_stocks = get_all_stocks()

    candidates = []  # 存储 (symbol, max_src_date)

    if symbol:
        # 单只模式: 不依赖 stocks 表, 直接按数据湖中是否存在报表判断
        if symbol not in get_financial_symbols():
            logger.warning(f"数据湖中无 {symbol} 的财务报表, 跳过")
            return
        logger.info(f"单只同步模式: {symbol}")
        calculator.calculate_for_symbol(symbol)
        return

    duckdb_conn = db_manager.get_duckdb_conn()
    db_manager.ensure_views("fin_income_statement", "fin_ttm")
    available_views = db_manager.list_available_views()

    if force_all:
        logger.info("强制全量模式...")
        # 候选集 = 数据湖中实际有报表的全部股票 (含孤儿股, 覆盖退市股),
        # 而非仅 stocks 活跃列表, 避免孤儿股 TTM 永不被重算
        candidates = [(s, "20991231") for s in get_financial_symbols()]
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

    if not candidates:
        logger.info("所有 TTM 数据已是最新。")
        return

    target_symbols = []
    for code, max_date in candidates:
        if force_all:
            target_symbols.append(code)
            continue
        required_reports = get_consecutive_reports(max_date, 5)
        check_sql = f"SELECT COUNT(DISTINCT report_date) FROM fin_income_statement WHERE symbol = '{code}' AND report_date IN {tuple(required_reports)}"
        count = duckdb_conn.execute(check_sql).fetchone()[0]
        if count == 5:
            target_symbols.append(code)
        else:
            logger.debug(
                f"跳过数据不全股票 {code}: 最新 {max_date} 往前 5 季仅有 {count} 季数据"
            )

    if not target_symbols:
        logger.info("数据完整性未达标，无需计算。")
        return

    symbol_name_map = {s[0]: s[1] for s in all_stocks}
    logger.info(f"开始为 {len(target_symbols)} 只股票同步 TTM 指标...")
    pbar = tqdm(target_symbols, desc="TTM 计算")
    for code in pbar:
        name = symbol_name_map.get(code, "")
        pbar.set_description(f"TTM 计算: {code} {name}")
        try:
            calculator.calculate_for_symbol(code)
        except Exception:
            logger.exception(f"{code} TTM 计算失败")


def sync_share_capital(symbol=None, force_all=False, start_date=None):
    """同步股本变动记录"""
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
        except Exception:
            failed += 1
            logger.error(f"{code} {name} 股本同步最终失败 (已重试)")
        time.sleep(random.uniform(1, 1.5))

    logger.info(
        f"股本同步完成: 共 {len(target_tasks)} 只, 本次同步 {len(target_tasks) - skipped - failed} 只, "
        f"跳过 {skipped} 只 (今日已同步), 失败 {failed} 只"
    )


def sync_daily_kline(symbol=None, force_all=False, start_date=None):
    """同步日线行情数据 (新浪源, 串行 + 保守节奏)"""
    from data_ingestion.collectors.kline_collector import DailyKlineCollector
    from utils.trade_date import get_latest_trade_date

    collector = DailyKlineCollector()
    all_stocks = get_all_stocks()
    target_tasks = [s for s in all_stocks if s[0] == symbol] if symbol else all_stocks

    if force_all and not start_date:
        start_date = "19900101"
        logger.info(f"强制全量模式：将从 {start_date} 开始同步 K 线数据")

    latest_date = get_latest_trade_date().strftime("%Y%m%d")
    logger.info(
        f"开始同步 {len(target_tasks)} 只股票的日线行情 (基准日期: {latest_date})..."
    )
    pbar = tqdm(target_tasks, desc="K线同步")
    for code, name in pbar:
        pbar.set_description(f"K线同步: {code} {name}")
        try:
            collector.collect_kline(code, start_date=start_date, end_date=latest_date)
        except Exception:
            logger.error(f"{code} {name} K线同步最终失败 (已重试)")
        time.sleep(random.uniform(2.0, 4.0))
    logger.info("K线同步完成")


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


def sync_all_data_flow(symbol=None, force_all=False):
    """执行全量数据同步流水线 (除元数据外)"""
    logger.info(">>> 开始执行一键数据同步流水线 <<<")
    try:
        # 先同步指标，因为指标表（东财源）的公告日期和更新日期更准确，用于后续修复三张表
        sync_financial_indicators(symbol=symbol, force_all=force_all)
        sync_financial_statements(symbol=symbol, force_all=force_all)
        calculate_ttm_metrics(symbol=symbol, force_all=force_all)
        sync_share_capital(symbol=symbol, force_all=force_all)
        sync_daily_kline(symbol=symbol, force_all=force_all)
    except SinaBlockedError as e:
        logger.error(f">>> 新浪接口 IP 风控，数据同步流水线中止 (已同步数据保留): {e}")
        logger.error(
            ">>> 请等待 5~60 分钟封禁解除后重试 (增量同步会自动跳过已完成部分) <<<"
        )
        return
    logger.info(">>> 数据同步流水线执行完成 <<<")


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
    from backtest.data_access import BacktestDataAccess
    from backtest.engine import DailyBacktestEngine
    from backtest.reporter import write_backtest_result
    from backtest.strategy_base import validate_target_weights
    from backtest.strategy_registry import get_backtest_strategy

    config = load_backtest_config(backtest_config_path)
    strategy = get_backtest_strategy(config.strategy_name)
    parameters = strategy.validate_parameters(config.strategy_parameters)
    config = config.with_resolved_strategy(strategy.metadata.version, parameters)
    data_access = BacktestDataAccess(db_manager)
    signal_data = strategy.load_signal_data(data_access, config, parameters)
    targets = validate_target_weights(
        strategy.build_targets(signal_data, config, parameters)
    )
    benchmark_prices = data_access.load_benchmark_prices(config)
    result = DailyBacktestEngine(config).run(signal_data, targets, benchmark_prices)
    output_dir = write_backtest_result(config, result.daily_nav, targets, result.trades)
    logger.info(f"回测完成，结果目录: {output_dir}")


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
    share_p.add_argument("--force-all", action="store_true", help="扫描所有活跃股票")

    # 7. sync-kline
    kline_p = subparsers.add_parser("sync-kline", help="同步日线行情数据")
    kline_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    kline_p.add_argument("--start-date", type=str, help="手动指定起始日期 (YYYYMMDD)")
    kline_p.add_argument("--force-all", action="store_true", help="扫描所有活跃股票")

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
    all_p = subparsers.add_parser("sync-all", help="一键同步全流程数据 (除元数据)")
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
        sync_daily_kline(
            symbol=args.symbol, force_all=args.force_all, start_date=args.start_date
        )
    elif args.command == "sync-etf-list":
        sync_etf_list()
    elif args.command == "sync-etf-kline":
        sync_etf_kline(
            symbol=args.symbol, force_all=args.force_all, start_date=args.start_date
        )
    elif args.command == "sync-all":
        sync_all_data_flow(symbol=args.symbol, force_all=args.force_all)
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
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("主程序执行异常退出")
    finally:
        db_manager.close_all()
