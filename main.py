import argparse
import time
import random
from concurrent.futures import ThreadPoolExecutor
from utils.logger import logger
from storage.database.manager import db_manager
from storage.database.financial_store import FinancialStore
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

def sync_financials(limit=None):
    """同步财务报表 (Phase 2)"""
    conn_sqlite = db_manager.get_sqlite_conn()
    cursor = conn_sqlite.cursor()
    # 按照代码获取待同步列表
    cursor.execute("SELECT code, name FROM stocks" + (f" LIMIT {limit}" if limit else ""))
    stocks = cursor.fetchall()
    
    if not stocks:
        logger.warning("没有发现可同步的股票")
        return

    logger.info(f"开始同步 {len(stocks)} 只股票的财务报表...")
    collector = FinancialCollector()
    store = FinancialStore()
    
    # 内部处理函数
    def process_stock(code, name):
        try:
            # 三大表
            for stat_type, table_name in [
                ("balance", "fin_balance_sheet"),
                ("profit", "fin_income_statement"),
                ("cashflow", "fin_cashflow_statement")
            ]:
                df = collector.fetch_statement(code, stat_type)
                if not df.empty:
                    store.save_statement(df, table_name)
            
            logger.info(f"[{code}] {name} 财务同步完成")
            time.sleep(random.uniform(0.2, 0.5))
        except Exception as e:
            logger.error(f"[{code}] {name} 同步失败: {e}")

    # 使用单线程同步财务数据，防止 DuckDB 在 ALTER TABLE 时产生事务冲突
    with ThreadPoolExecutor(max_workers=1) as executor:
        for code, name in stocks:
            executor.submit(process_stock, code, name)

def main():
    parser = argparse.ArgumentParser(description="QuantPyLab 实验室入口")
    parser.add_argument("--sync-stocks", action="store_true", help="同步股票列表")
    parser.add_argument("--enrich-metadata", action="store_true", help="补全元数据")
    parser.add_argument("--sync-fin", action="store_true", help="同步财务报表")
    parser.add_argument("--limit", type=int, help="限制处理数量")
    
    args = parser.parse_args()
    if args.sync_stocks: sync_stock_list()
    elif args.enrich_metadata: enrich_stock_metadata(limit=args.limit)
    elif args.sync_fin: sync_financials(limit=args.limit)
    else: parser.print_help()

if __name__ == "__main__":
    try: main()
    finally: db_manager.close_all()
