import pandas as pd
import os
from pathlib import Path
from utils.logger import logger
from storage.database.manager import db_manager
from storage.file_store.parquet_store import ParquetStore
from config.settings import WAREHOUSE_DIR

class IndicatorStore:
    """
    财务指标存储器 (Analysis Layer)：已升级为基于 Parquet 数据湖的存储模式。
    """

    def __init__(self):
        self.conn = db_manager.get_duckdb_conn()
        self.parquet_store = ParquetStore()
        self.category = "indicators"

    def save_indicators(self, df: pd.DataFrame):
        """
        保存指标数据到 Parquet。
        """
        if df.empty:
            return

        try:
            symbol = df['symbol'].iloc[0]
            
            # 处理增量合并
            target_file = Path(WAREHOUSE_DIR) / self.category / f"symbol={symbol}" / "data.parquet"
            if target_file.exists():
                existing_df = pd.read_parquet(target_file)
                # 合并并去重，以 report_date 为准
                combined_df = pd.concat([existing_df, df.drop(columns=['symbol'], errors='ignore')], ignore_index=True)
                df = combined_df.drop_duplicates(subset=['report_date'], keep='last')
            
            df['symbol'] = symbol
            self.parquet_store.save_partition(df, self.category, symbol)
            logger.info(f"成功入库 {symbol}: {len(df)} 条指标记录 (Parquet)")
            
        except Exception as e:
            logger.error(f"存储财务指标失败: {e}")
            raise

    def get_existing_report_dates(self) -> set:
        """获取数据库中已有的 {symbol}_{report_date} 集合"""
        path = self.parquet_store.get_path(self.category)
        if not any(Path(WAREHOUSE_DIR).glob(f"{self.category}/*/data.parquet")):
            return set()
            
        res = self.conn.execute(f"SELECT symbol || '_' || report_date FROM read_parquet('{path}', hive_partitioning=1)").fetchall()
        return set([row[0] for row in res])

    def get_stocks_without_indicators(self, all_codes: list) -> list:
        """从全量列表中筛选出在数据库中完全没有任何指标记录的股票"""
        path = self.parquet_store.get_path(self.category)
        if not any(Path(WAREHOUSE_DIR).glob(f"{self.category}/*/data.parquet")):
            return all_codes
        
        existing_symbols = self.conn.execute(f"SELECT DISTINCT symbol FROM read_parquet('{path}', hive_partitioning=1)").fetchall()
        existing_symbols_set = set([row[0] for row in existing_symbols])
        
        return [c for c in all_codes if c not in existing_symbols_set]

    def get_existing_dates(self, symbol: str) -> set:
        """获取某只股票已有的报告期列表"""
        target_file = Path(WAREHOUSE_DIR) / self.category / f"symbol={symbol}" / "data.parquet"
        if not target_file.exists():
            return set()
        
        # 对于单股，直接用 pandas 读更快
        df = pd.read_parquet(target_file)
        return set(df['report_date'].tolist())
