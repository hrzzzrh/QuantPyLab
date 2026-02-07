import argparse
import time
import random
from utils.logger import logger
from storage.database.manager import db_manager
from data_ingestion.collectors.stock_list import StockListCollector, StockDetailCollector
from data_ingestion.collectors.industry_collector import HighSpeedIndustryCollector

def sync_stock_list():
    """同步基础股票列表 (Phase 1)"""
    collector = StockListCollector()
    df = collector.fetch_all_stocks()
    
    if df.empty:
        logger.error("同步失败：未能获取到有效数据")
        return

    try:
        conn = db_manager.get_sqlite_conn()
        cursor = conn.cursor()
        logger.info("清理旧股票列表数据...")
        cursor.execute("DELETE FROM stocks")
        df.to_sql('stocks', conn, if_exists='append', index=False)
        conn.commit()
        logger.info(f"成功同步 {len(df)} 条记录到元数据库 (stocks)")
    except Exception as e:
        logger.error(f"数据库操作失败: {str(e)}")
        if 'conn' in locals():
            conn.rollback()

def enrich_stock_metadata(limit=None):
    """补全股票元数据 (Phase 1.5: High-Speed Hybrid Strategy)"""
    conn = db_manager.get_sqlite_conn()
    
    # --- Stage 1: 快速全量刷新行业 (HighSpeed) ---
    logger.info(">>> Stage 1: 开始快速同步全市场行业信息...")
    industry_collector = HighSpeedIndustryCollector()
    industry_collector.sync_industries(conn)
    
    # --- Stage 2: 增量补全地域和上市日期 (Hybrid) ---
    # 只需要筛选 area 或 list_date 为空的股票
    logger.info(">>> Stage 2: 开始增量补全地域和上市日期...")
    cursor = conn.cursor()
    query = "SELECT symbol, code, name FROM stocks WHERE area IS NULL OR list_date IS NULL"
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    pending_stocks = cursor.fetchall()
    
    if not pending_stocks:
        logger.info("所有股票元数据已是最新，无需补全。")
        return

    logger.info(f"待处理 {len(pending_stocks)} 只股票...")
    detail_collector = StockDetailCollector()
    
    count = 0
    for symbol, code, name in pending_stocks:
        try:
            # 优先从雪球拿地域和上市日期
            info = {}
            if symbol.startswith(('sh', 'sz')): # 雪球只支持沪深
                info = detail_collector.fetch_from_xueqiu(symbol)
            
            # 如果雪球没拿到上市日期（或它是 BJ），尝试东财个股接口
            if not info.get('list_date'):
                em_info = detail_collector.fetch_from_eastmoney(code)
                if em_info.get('list_date'):
                    info['list_date'] = em_info.get('list_date')
            
            # 执行更新 (注意：不再更新 industry，因为 Stage 1 已经做了)
            if info:
                cursor.execute("""
                    UPDATE stocks 
                    SET area = ?, list_date = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE symbol = ?
                """, (info.get('area'), info.get('list_date'), symbol))
                
                count += 1
                if count % 20 == 0:
                    conn.commit()
                    logger.info(f"Stage 2 进度: {count}/{len(pending_stocks)}...")
            
            time.sleep(random.uniform(0.3, 0.6))
            
        except Exception as e:
            logger.error(f"处理 {symbol} 失败: {e}")
            continue

    conn.commit()
    logger.info(f"Stage 2 完成，共处理 {count} 条记录。")

def main():
    parser = argparse.ArgumentParser(description="QuantPyLab 实验室入口")
    parser.add_argument("--sync-stocks", action="store_true", help="同步 A 股股票列表 (基础)")
    parser.add_argument("--enrich-metadata", action="store_true", help="补全股票元数据 (行业、地域、上市日期)")
    parser.add_argument("--limit", type=int, help="限制补全的股票数量 (仅用于测试)")
    
    args = parser.parse_args()

    if args.sync_stocks:
        sync_stock_list()
    elif args.enrich_metadata:
        enrich_stock_metadata(limit=args.limit)
    else:
        parser.print_help()

if __name__ == "__main__":
    try:
        main()
    finally:
        db_manager.close_all()
