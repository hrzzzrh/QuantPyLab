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
            
            # --- 日期对齐修复逻辑开始 ---
            # 从 fin_indicator (东财源) 获取更准确的公告日期和更新日期
            try:
                # 检查 fin_indicator 视图是否已加载
                available_views = db_manager.list_available_views()
                if "fin_indicator" in available_views:
                    sql = f"SELECT report_date, \"公告日期\" as em_ann_date, \"更新日期\" as em_up_date FROM fin_indicator WHERE symbol = '{symbol}'"
                    df_dates = self.conn.execute(sql).df()
                    
                    if not df_dates.empty:
                        # 标准化东财日期格式
                        df_dates['em_ann_date'] = pd.to_datetime(df_dates['em_ann_date']).dt.strftime('%Y%m%d')
                        df_dates['em_up_date'] = pd.to_datetime(df_dates['em_up_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 合并到当前报表 df
                        # 注意：优先信任指标表的日期，强制覆盖
                        df = pd.merge(df, df_dates, on='report_date', how='left')
                        
                        # 执行覆盖
                        if 'em_ann_date' in df.columns:
                            mask = df['em_ann_date'].notna()
                            df.loc[mask, '公告日期'] = df.loc[mask, 'em_ann_date']
                            df.drop(columns=['em_ann_date'], inplace=True)
                            
                        if 'em_up_date' in df.columns:
                            mask = df['em_up_date'].notna()
                            df.loc[mask, '更新日期'] = df.loc[mask, 'em_up_date']
                            df.drop(columns=['em_up_date'], inplace=True)
                            
                        logger.debug(f"已根据 fin_indicator 为 {symbol} 修复了公告/更新日期")
            except Exception as date_err:
                logger.warning(f"尝试修复 {symbol} 报表日期失败 (跳过): {date_err}")
            # --- 日期对齐修复逻辑结束 ---

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
