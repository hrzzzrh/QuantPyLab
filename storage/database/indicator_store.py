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

    def _enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        强制 Schema 一致性：
        1. 元数据列强制为 VARCHAR (str)
        2. 指标列强制为 DOUBLE (float64)
        """
        if df.empty:
            return df
            
        # 定义元数据列 (需保持为字符串)
        meta_cols = {
            'report_date', '证券代码', '股票代码', '股票简称', 
            '机构代码', '机构类型', '报告类型', '报告期名称', 
            '证券类型代码', '公告日期', '更新日期', '币种', 
            '其他_REPORT_YEAR', 'symbol'
        }
        
        df = df.copy()
        for col in df.columns:
            if col in meta_cols:
                # 转换为字符串，并将空值替换为空字符串，避免 Pandas 存为 object/NaN 导致的混乱
                df[col] = df[col].astype(str).replace(['nan', 'None', 'NaT', '<NA>'], '')
            else:
                # 所有的财务指标列，强制转换为数值型 (float64)
                # errors='coerce' 会将无法转换的(如空字符串)转为 NaN
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        
        return df

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
                df = combined_df.drop_duplicates(subset=['report_date'], keep='last').copy()
            
            df['symbol'] = symbol
            
            # 【核心修复】写入前强制执行 Schema 一致性
            df = self._enforce_schema(df)
            
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
            
        res = self.conn.execute(f"SELECT symbol || '_' || report_date FROM read_parquet('{path}', hive_partitioning=1, union_by_name=1)").fetchall()
        return set([row[0] for row in res])

    def get_stocks_without_indicators(self, all_codes: list) -> list:
        """从全量列表中筛选出在数据库中完全没有任何指标记录的股票"""
        path = self.parquet_store.get_path(self.category)
        if not any(Path(WAREHOUSE_DIR).glob(f"{self.category}/*/data.parquet")):
            return all_codes
        
        existing_symbols = self.conn.execute(f"SELECT DISTINCT symbol FROM read_parquet('{path}', hive_partitioning=1, union_by_name=1)").fetchall()
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
