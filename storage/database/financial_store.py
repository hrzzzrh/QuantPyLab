import pandas as pd
import os
from pathlib import Path
from utils.logger import logger
from storage.database.manager import db_manager
from storage.file_store.parquet_store import ParquetStore
from config.settings import WAREHOUSE_DIR

class FinancialStore:
    """
    财务数据存储器：已升级为基于 Parquet 数据湖的存储模式。
    支持非阻塞并发读写。
    """

    def __init__(self):
        self.conn = db_manager.get_duckdb_conn()
        self.parquet_store = ParquetStore()
        # 表名到目录的映射
        self.table_map = {
            "fin_balance_sheet": "financial_statements/type=balance",
            "fin_income_statement": "financial_statements/type=income",
            "fin_cashflow_statement": "financial_statements/type=cashflow"
        }

    def save_statement(self, df: pd.DataFrame, table_name: str):
        """
        保存报表数据到 Parquet。
        """
        if df.empty:
            return

        try:
            category = self.table_map.get(table_name)
            if not category:
                raise ValueError(f"未知表名: {table_name}")

            symbol = df['symbol'].iloc[0]
            
            # 读取该股票已有的数据（如果存在），进行合并去重
            # 这样可以处理增量更新/覆盖逻辑
            target_file = Path(WAREHOUSE_DIR) / category / f"symbol={symbol}" / "data.parquet"
            if target_file.exists():
                existing_df = pd.read_parquet(target_file)
                # 合并并按 symbol/report_date 去重
                # 注意：existing_df 可能没有 symbol 列，因为我们存的时候删掉了，但在读取时如果是通过 duckdb 会有。
                # 直接用 pandas 读，需要补回 symbol 列或按 report_date 合并
                combined_df = pd.concat([existing_df, df.drop(columns=['symbol'], errors='ignore')], ignore_index=True)
                # 以 report_date 为准去重，保留最新的
                df = combined_df.drop_duplicates(subset=['report_date'], keep='last').copy()
            
            # 重新补上 symbol 供存储逻辑识别（虽然存储时会再删掉，但为了逻辑统一）
            df['symbol'] = symbol
            self.parquet_store.save_partition(df, category, symbol)
            
        except Exception:
            logger.exception(f"存储 {table_name} 失败")
            raise

    def get_existing_report_dates(self) -> set:
        """获取三张表都存在的 {symbol}_{report_date} 交集"""
        sets = []
        for table_name, category in self.table_map.items():
            path = self.parquet_store.get_path(category)
            if not any(Path(WAREHOUSE_DIR).glob(f"{category}/*/data.parquet")):
                return set()
            
            # 使用 DuckDB 扫描 Parquet 极其高效
            res = self.conn.execute(f"SELECT symbol || '_' || report_date FROM read_parquet('{path}', hive_partitioning=1, union_by_name=1)").fetchall()
            sets.append(set([row[0] for row in res]))
        
        return set.intersection(*sets) if sets else set()

    def get_stocks_without_financials(self, all_codes: list) -> list:
        """从全量列表中筛选出财务报表不完整的股票"""
        incomplete_codes = set()
        
        for table_name, category in self.table_map.items():
            path = self.parquet_store.get_path(category)
            if not any(Path(WAREHOUSE_DIR).glob(f"{category}/*/data.parquet")):
                return all_codes
            
            existing_codes = self.conn.execute(f"SELECT DISTINCT symbol FROM read_parquet('{path}', hive_partitioning=1, union_by_name=1)").fetchall()
            existing_codes_set = set([row[0] for row in existing_codes])
            
            for c in all_codes:
                if c not in existing_codes_set:
                    incomplete_codes.add(c)
        
        return list(incomplete_codes)
