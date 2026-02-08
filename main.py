import argparse
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from utils.logger import logger
from storage.database.manager import db_manager
from storage.database.financial_store import FinancialStore
from storage.database.indicator_store import IndicatorStore
from data_ingestion.collectors.stock_list import StockListCollector, StockDetailCollector
from data_ingestion.collectors.industry_collector import HighSpeedIndustryCollector
from data_ingestion.collectors.financial_collector import FinancialCollector

def sync_stock_list():
    """同步基础股票列表 (Phase 1)"""
    collector = StockListCollector()
    df = collector.fetch_all_stocks()
    if df.empty: return
    conn = db_manager.get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stocks")
    df.to_sql('stocks', conn, if_exists='append', index=False)
    conn.commit()
    logger.info(f"成功同步 {len(df)} 条记录到 stocks 表")

def enrich_stock_metadata(limit=None):
    """补全股票元数据 (Phase 1.5)"""
    conn = db_manager.get_sqlite_conn()
    industry_collector = HighSpeedIndustryCollector()
    industry_collector.sync_industries(conn)
    
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, code FROM stocks WHERE area IS NULL OR list_date IS NULL" + (f" LIMIT {limit}" if limit else ""))
    pending = cursor.fetchall()
    if not pending: return
    
    detail_collector = StockDetailCollector()
    for symbol, code in pending:
        try:
            info = detail_collector.fetch_from_xueqiu(symbol) if symbol.startswith(('sh', 'sz')) else {}
            if not info.get('list_date'):
                em_info = detail_collector.fetch_from_eastmoney(code)
                info['list_date'] = em_info.get('list_date')
            if info:
                cursor.execute("UPDATE stocks SET area = ?, list_date = ?, updated_at = CURRENT_TIMESTAMP WHERE symbol = ?", (info.get('area'), info.get('list_date'), symbol))
            time.sleep(random.uniform(0.3, 0.6))
        except Exception as e: logger.error(f"处理 {symbol} 失败: {e}")
    conn.commit()

def get_target_report_dates():
    """计算最近 4 个报告期"""
    today = datetime.now()
    dates = []
    # 财务报告固定在 3, 6, 9, 12 月底
    year = today.year
    for i in range(4):
        month = ((today.month - 1) // 3 - i) * 3
        curr_year = year
        while month <= 0:
            month += 12
            curr_year -= 1
        day = 31 if month in [3, 12] else 30
        dates.append(f"{curr_year}{month:02d}{day}")
    return dates

def sync_financials(limit=None, force_all=False):
    """
    同步财务报表 (Phase 2: Smart Version)
    """
    store = FinancialStore()
    collector = FinancialCollector()
    conn_sqlite = db_manager.get_sqlite_conn()
    
    target_codes = set() # 最终待同步的代码集合

    if force_all:
        logger.info("强制全量模式：将扫描所有股票...")
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT code FROM stocks" + (f" LIMIT {limit}" if limit else ""))
        target_codes = {row[0] for row in cursor.fetchall()}
    else:
        # --- 路径 1: 披露日历驱动 (Smart Sync) ---
        report_dates = get_target_report_dates()
        existing_records = store.get_existing_report_dates()
        
        logger.info(f"智能增量模式：检查最近报告期 {report_dates}...")
        for r_date in report_dates:
            df_plans = collector.get_disclosure_plans(r_date)
            if not df_plans.empty:
                # 筛选：实际已披露 且 数据库中没有
                df_plans['actual_date'] = pd.to_datetime(df_plans['actual_date'], errors='coerce')
                disclosed = df_plans[df_plans['actual_date'].notna()]
                
                for _, row in disclosed.iterrows():
                    key = f"{row['code']}_{r_date}"
                    if key not in existing_records:
                        target_codes.add(row['code'])
        
        logger.info(f"日历驱动发现 {len(target_codes)} 只股票需要同步。")

        # --- 路径 2: 漏检补偿 (扫除孤儿股) ---
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT code FROM stocks")
        all_codes = [row[0] for row in cursor.fetchall()]
        orphans = store.get_stocks_without_financials(all_codes)
        
        if orphans:
            logger.info(f"漏检扫描发现 {len(orphans)} 只孤儿股（从未同步过），加入队列。")
            target_codes.update(orphans[:100] if not limit else orphans[:limit]) # 每次补偿 100 只

    if not target_codes:
        logger.info("所有财务数据已是最新。")
        return

    # 限制处理数量 (用于测试)
    final_list = list(target_codes)
    if limit and len(final_list) > limit:
        final_list = final_list[:limit]

    logger.info(f"最终待同步任务数: {len(final_list)}")
    
    # 串行执行 (为了 DuckDB ALTER TABLE 安全)
    count = 0
    for code in final_list:
        try:
            for stat_type, table_name in [
                ("balance", "fin_balance_sheet"),
                ("profit", "fin_income_statement"),
                ("cashflow", "fin_cashflow_statement")
            ]:
                df = collector.fetch_statement(code, stat_type)
                if not df.empty:
                    store.save_statement(df, table_name)
                time.sleep(random.uniform(2, 3))
            
            count += 1
            logger.info(f"[{count}/{len(final_list)}] {code} 同步完成")
        except Exception as e:
            logger.error(f"{code} 同步失败: {e}")
            return

def sync_indicators(limit=None, symbol=None, force_all=False):
    """
    同步财务指标 (Phase 2.5: Smart Version)
    """
    store = IndicatorStore()
    collector = FinancialCollector()
    conn_sqlite = db_manager.get_sqlite_conn()
    
    target_tasks = [] # 最终待同步任务列表 [(code, m_symbol)]

    if symbol:
        # 路径 0: 单只强制同步
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT code, symbol FROM stocks WHERE code = ? OR symbol = ?", (symbol, symbol))
        row = cursor.fetchone()
        if row:
            target_tasks.append((row[0], row[1]))
        else:
            logger.error(f"找不到代码: {symbol}")
            return
    elif force_all:
        logger.info("强制全量模式：扫描所有活跃股票...")
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT code, symbol FROM stocks WHERE is_active = 1" + (f" LIMIT {limit}" if limit else ""))
        target_tasks = cursor.fetchall()
    else:
        # --- 路径 1: 披露日历驱动 (Smart Sync) ---
        report_dates = get_target_report_dates()
        existing_records = store.get_existing_report_dates()
        
        target_codes = set()
        logger.info(f"智能增量模式：检查最近报告期 {report_dates}...")
        for r_date in report_dates:
            df_plans = collector.get_disclosure_plans(r_date)
            if not df_plans.empty:
                # 筛选：实际已披露 且 数据库中没有
                df_plans['actual_date'] = pd.to_datetime(df_plans['actual_date'], errors='coerce')
                disclosed = df_plans[df_plans['actual_date'].notna()]
                
                for _, row in disclosed.iterrows():
                    key = f"{row['code']}_{r_date}"
                    if key not in existing_records:
                        target_codes.add(row['code'])
        
        logger.info(f"日历驱动发现 {len(target_codes)} 只股票需要同步指标。")

        # --- 路径 2: 漏检补偿 (扫除孤儿股) ---
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT code, symbol FROM stocks WHERE is_active = 1")
        all_stocks = cursor.fetchall() # [(code, m_symbol), ...]
        all_codes = [s[0] for s in all_stocks]
        
        orphans_codes = store.get_stocks_without_indicators(all_codes)
        if orphans_codes:
            logger.info(f"漏检扫描发现 {len(orphans_codes)} 只孤儿股（从未同步过指标），加入队列。")
            orphans_codes_set = set(orphans_codes[:100] if not limit else orphans_codes[:limit])
            target_codes.update(orphans_codes_set)

        # 构建最终任务列表
        code_to_m_symbol = {s[0]: s[1] for s in all_stocks}
        for c in target_codes:
            if c in code_to_m_symbol:
                target_tasks.append((c, code_to_m_symbol[c]))

    if not target_tasks:
        logger.info("所有财务指标已是最新。")
        return

    # 限制处理数量 (用于测试)
    if limit and len(target_tasks) > limit:
        target_tasks = target_tasks[:limit]

    logger.info(f"最终待同步任务数: {len(target_tasks)}")
    
    count = 0
    for code, m_symbol in target_tasks:
        try:
            fmt_symbol = m_symbol.upper()
            if not ('.' in fmt_symbol):
                 if fmt_symbol.startswith(('SH', 'SZ', 'BJ')):
                     pre = fmt_symbol[:2]
                     suf = fmt_symbol[2:]
                     fmt_symbol = f"{suf}.{pre}"
            
            collector.collect_indicators(code, fmt_symbol)
            count += 1
            if count % 10 == 0:
                logger.info(f"已进度: [{count}/{len(target_tasks)}]")
            
            time.sleep(random.uniform(0.5, 1.0))
        except Exception as e:
            logger.error(f"{code} 同步指标失败: {e}")

def main():
    parser = argparse.ArgumentParser(description="QuantPyLab 实验室入口")
    parser.add_argument("--sync-stocks", action="store_true", help="同步股票列表")
    parser.add_argument("--enrich-metadata", action="store_true", help="补全元数据")
    parser.add_argument("--sync-fin", action="store_true", help="同步财务报表 (智能增量)")
    parser.add_argument("--sync-indicators", action="store_true", help="同步财务指标 (东财)")
    parser.add_argument("--symbol", type=str, help="指定同步单只股票指标")
    parser.add_argument("--force-all", action="store_true", help="配合 --sync-fin，强制全量扫描所有股票")
    parser.add_argument("--limit", type=int, help="限制处理数量")
    
    args = parser.parse_args()
    if args.sync_stocks: sync_stock_list()
    elif args.enrich_metadata: enrich_stock_metadata(limit=args.limit)
    elif args.sync_fin: sync_financials(limit=args.limit, force_all=args.force_all)
    elif args.sync_indicators: sync_indicators(limit=args.limit, symbol=args.symbol, force_all=args.force_all)
    else: parser.print_help()

if __name__ == "__main__":
    try: main()
    finally: db_manager.close_all()
