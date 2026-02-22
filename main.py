import argparse
import time
import random
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from utils.logger import logger
from utils.financial import get_consecutive_reports
from config.settings import WAREHOUSE_DIR
from storage.database.manager import db_manager
from storage.database.financial_store import FinancialStore
from storage.database.indicator_store import IndicatorStore
from data_ingestion.collectors.stock_list import StockListCollector, StockDetailCollector
from data_ingestion.collectors.industry_collector import HighSpeedIndustryCollector
from data_ingestion.collectors.financial_collector import FinancialCollector

# --- 辅助函数 ---

def get_active_stocks():
    """获取所有活跃股票的 (code, symbol)"""
    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT code, symbol FROM stocks WHERE is_active = 1")
    return cursor.fetchall()

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
    """同步基础股票列表 (stocks 表)"""
    collector = StockListCollector()
    df = collector.fetch_all_stocks()
    if df.empty: return
    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stocks")
    df.to_sql('stocks', conn, if_exists='append', index=False)
    conn.commit()
    logger.info(f"成功同步 {len(df)} 条记录到 stocks 表")

def sync_stock_metadata(run_industry=True, run_list_info=True):
    """补全股票元数据 (行业、上市日期等)"""
    conn = db_manager.get_sqlite_conn()
    if run_industry:
        logger.info("--- 正在批量同步行业信息 ---")
        HighSpeedIndustryCollector().sync_industries(conn)
    
    if run_list_info:
        logger.info("--- 正在补全个股上市详情 (地域、日期) ---")
        cursor = conn.cursor()
        cursor.execute("SELECT symbol, code FROM stocks WHERE area IS NULL OR list_date IS NULL")
        pending = cursor.fetchall()
        if not pending: return
        
        detail_collector = StockDetailCollector()
        for symbol, code in tqdm(pending, desc="补全详情"):
            try:
                info = detail_collector.fetch_from_xueqiu(symbol) if symbol.startswith(('sh', 'sz')) else {}
                if not info.get('list_date'):
                    info['list_date'] = detail_collector.fetch_from_eastmoney(code).get('list_date')
                if info:
                    cursor.execute("UPDATE stocks SET area = ?, list_date = ?, updated_at = CURRENT_TIMESTAMP WHERE symbol = ?", (info.get('area'), info.get('list_date'), symbol))
                time.sleep(random.uniform(0.2, 0.4))
            except Exception:
                logger.debug(f"补全 {symbol} 详情失败", exc_info=True)
            if tqdm.get_lock().locks: conn.commit()
        conn.commit()

def get_target_report_dates():
    today = datetime.now()
    dates = []
    for i in range(4):
        month = ((today.month - 1) // 3 - i) * 3
        curr_year = today.year
        while month <= 0: month += 12; curr_year -= 1
        dates.append(f"{curr_year}{month:02d}{31 if month in [3, 12] else 30}")
    return dates

def sync_financial_statements(symbol=None, force_all=False):
    """同步财务三大报表"""
    store = FinancialStore()
    collector = FinancialCollector()
    all_active = get_active_stocks()
    all_codes = [s[0] for s in all_active]
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
                df['actual_date'] = pd.to_datetime(df['actual_date'], errors='coerce')
                for code in df[df['actual_date'].notna()]['code']:
                    if f"{code}_{r_date}" not in existing: target_codes.add(code)
        target_codes.update(get_orphan_codes("financial", all_codes))

    if not target_codes:
        logger.info("财务报表数据已是最新。")
        return

    for code in tqdm(list(target_codes), desc="报表同步"):
        try:
            stat_map = {"balance": "fin_balance_sheet", "profit": "fin_income_statement", "cashflow": "fin_cashflow_statement"}
            for st, table_name in stat_map.items():
                df = collector.fetch_statement(code, st)
                if not df.empty: store.save_statement(df, table_name)
                time.sleep(random.uniform(1.0, 2.0))
        except Exception:
            logger.exception(f"{code} 报表同步失败")

def sync_financial_indicators(symbol=None, force_all=False):
    """同步东财财务指标"""
    store = IndicatorStore()
    collector = FinancialCollector()
    all_active = get_active_stocks()
    target_tasks = []

    if symbol:
        target_tasks = [(s[0], s[1]) for s in all_active if s[0] == symbol or s[1] == symbol]
    elif force_all:
        target_tasks = all_active
    else:
        report_dates = get_target_report_dates()
        existing = store.get_existing_report_dates()
        target_codes = set()
        for r_date in report_dates:
            df = collector.get_disclosure_plans(r_date)
            if not df.empty:
                df['actual_date'] = pd.to_datetime(df['actual_date'], errors='coerce')
                for code in df[df['actual_date'].notna()]['code']:
                    if f"{code}_{r_date}" not in existing: target_codes.add(code)
        target_codes.update(get_orphan_codes("indicators", [s[0] for s in all_active]))
        code_map = {s[0]: s[1] for s in all_active}
        target_tasks = [(c, code_map[c]) for c in target_codes if c in code_map]

    if not target_tasks:
        logger.info("指标数据已是最新。")
        return

    for code, m_symbol in tqdm(target_tasks, desc="指标同步"):
        try:
            fmt_symbol = m_symbol.upper()
            if '.' not in fmt_symbol and fmt_symbol.startswith(('SH', 'SZ', 'BJ')):
                fmt_symbol = f"{fmt_symbol[2:]}.{fmt_symbol[:2]}"
            collector.collect_indicators(code, fmt_symbol)
            time.sleep(random.uniform(0.5, 1.0))
        except Exception:
            logger.exception(f"{code} 指标同步失败")

def calculate_ttm_metrics(symbol=None, force_all=False):
    """计算滚动十二个月 (TTM) 财务指标"""
    from analysis.processors.ttm_calculator import TTMCalculator
    calculator = TTMCalculator()
    all_active = get_active_stocks()
    
    candidates = [] # 存储 (symbol, max_src_date)

    if symbol:
        logger.info(f"单只同步模式: {symbol}")
        target_symbols = [s[0] for s in all_active if s[0] == symbol or s[1] == symbol]
        for code in target_symbols:
            calculator.calculate_for_symbol(code)
        return

    duckdb_conn = db_manager.get_duckdb_conn()
    available_views = db_manager.list_available_views()
    
    if force_all:
        logger.info("强制全量模式...")
        # 直接使用 get_active_stocks() 获取的 all_active 列表，无需再查 DuckDB
        candidates = [(s[0], '20991231') for s in all_active]
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
            logger.debug(f"跳过数据不全股票 {code}: 最新 {max_date} 往前 5 季仅有 {count} 季数据")

    if not target_symbols:
        logger.info("数据完整性未达标，无需计算。")
        return

    logger.info(f"开始为 {len(target_symbols)} 只股票同步 TTM 指标...")
    for code in tqdm(target_symbols, desc="TTM 计算"):
        try: calculator.calculate_for_symbol(code)
        except Exception:
            logger.exception(f"{code} TTM 计算失败")

def sync_share_capital(symbol=None, force_all=False, start_date=None):
    """同步股本变动记录"""
    from data_ingestion.collectors.share_collector import ShareCollector
    collector = ShareCollector()
    all_active = get_active_stocks()
    target_codes = [s[0] for s in all_active if s[0] == symbol or s[1] == symbol] if symbol else [s[0] for s in all_active]
    
    logger.info(f"开始同步 {len(target_codes)} 只股票的股本变动...")
    for code in tqdm(target_codes, desc="股本同步"):
        collector.collect_share_capital(code, start_date=start_date)
        time.sleep(random.uniform(1, 1.5))

def sync_daily_kline(symbol=None, force_all=False, start_date=None):
    """同步日线行情数据"""
    from data_ingestion.collectors.kline_collector import DailyKlineCollector
    collector = DailyKlineCollector()
    all_active = get_active_stocks()
    target_codes = [s[0] for s in all_active if s[0] == symbol or s[1] == symbol] if symbol else [s[0] for s in all_active]
    
    logger.info(f"开始同步 {len(target_codes)} 只股票的日线行情...")
    for code in tqdm(target_codes, desc="K线同步"):
        collector.collect_kline(code, start_date=start_date)

def sync_all_data_flow(symbol=None, force_all=False):
    """执行全量数据同步流水线 (除元数据外)"""
    logger.info(">>> 开始执行一键数据同步流水线 <<<")
    # 先同步指标，因为指标表（东财源）的公告日期和更新日期更准确，用于后续修复三张表
    sync_financial_indicators(symbol=symbol, force_all=force_all)
    sync_financial_statements(symbol=symbol, force_all=force_all)
    calculate_ttm_metrics(symbol=symbol, force_all=force_all)
    sync_share_capital(symbol=symbol, force_all=force_all)
    sync_daily_kline(symbol=symbol, force_all=force_all)
    logger.info(">>> 数据同步流水线执行完成 <<<")

def export_duckdb_views(output_path: str):
    """导出 DuckDB 视图的 SQL 定义"""
    sql = db_manager.generate_full_sql()
    Path(output_path).write_text(sql, encoding='utf-8')
    logger.info(f"视图脚本已导出至: {output_path}")

# --- CLI 定义 ---

def main():
    parser = argparse.ArgumentParser(description="QuantPyLab 实验室统一入口", formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", help="子命令集")

    # 1. sync-stocks
    subparsers.add_parser("sync-stocks", help="同步 A 股全量股票代码与名称 (stocks表)")

    # 2. sync-metadata
    meta_p = subparsers.add_parser("sync-metadata", help="同步股票行业、地域、上市日期等元数据")
    meta_p.add_argument("--industry", action="store_true", help="仅同步行业")
    meta_p.add_argument("--list-info", action="store_true", help="仅同步上市详情")

    # 3. sync-financial
    fin_p = subparsers.add_parser("sync-financial", help="同步历史财务报表")
    fin_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    fin_p.add_argument("--force-all", action="store_true", help="全量扫描所有股票")

    # 4. sync-indicators
    ind_p = subparsers.add_parser("sync-indicators", help="同步东方财富 140+ 项财务指标")
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

    # 8. sync-all
    all_p = subparsers.add_parser("sync-all", help="一键同步全流程数据 (除元数据)")
    all_p.add_argument("--symbol", type=str, help="指定单只股票代码")
    all_p.add_argument("--force-all", action="store_true", help="全量强制同步")

    # 9. export-views
    exp_p = subparsers.add_parser("export-views", help="导出 DuckDB 视图的 SQL 定义脚本")
    exp_p.add_argument("--output", "-o", type=str, default="docs/view_definition.sql", help="输出文件路径 (默认: docs/view_definition.sql)")

    # 10. show-views
    subparsers.add_parser("show-views", help="显示视图依赖关系图 (PlantUML)")

    args = parser.parse_args()

    if args.command == "sync-stocks":
        sync_stock_list()
    elif args.command == "sync-metadata":
        sync_stock_metadata(run_industry=not args.list_info, run_list_info=not args.industry)
    elif args.command == "sync-financial":
        sync_financial_statements(symbol=args.symbol, force_all=args.force_all)
    elif args.command == "sync-indicators":
        sync_financial_indicators(symbol=args.symbol, force_all=args.force_all)
    elif args.command == "calc-ttm":
        calculate_ttm_metrics(symbol=args.symbol, force_all=args.force_all)
    elif args.command == "sync-share":
        sync_share_capital(symbol=args.symbol, force_all=args.force_all, start_date=args.start_date)
    elif args.command == "sync-kline":
        sync_daily_kline(symbol=args.symbol, force_all=args.force_all, start_date=args.start_date)
    elif args.command == "sync-all":
        sync_all_data_flow(symbol=args.symbol, force_all=args.force_all)
    elif args.command == "export-views":
        export_duckdb_views(args.output)
    elif args.command == "show-views":
        puml = db_manager.get_view_relationships_puml()
        print("\n--- PlantUML Source ---")
        print(puml)
        print("\n(You can copy this source to https://www.plantuml.com/plantuml/ to view the graph)\n")
    else:
        parser.print_help()

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("主程序执行异常退出")
    finally:
        db_manager.close_all()
