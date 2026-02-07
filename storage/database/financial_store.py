import pandas as pd
from utils.logger import logger
from storage.database.manager import db_manager

class FinancialStore:
    """
    财务数据存储器：负责 DuckDB 的写入与 Schema 校验。
    """

    def __init__(self):
        self.conn = db_manager.get_duckdb_conn()

    def save_statement(self, df: pd.DataFrame, table_name: str):
        """
        将报表数据存入 DuckDB，包含严格的 Schema 校验。
        """
        if df.empty:
            return

        try:
            # 1. 检查表是否存在
            table_exists = self._check_table_exists(table_name)
            
            if not table_exists:
                # 首次建表：直接写入
                logger.info(f"创建新表 {table_name}...")
                df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                return

            # 2. 严格 Schema 校验
            self._validate_schema(df, table_name)

            # 3. 增量写入 (处理 Upsert 逻辑)
            symbol = df['symbol'].iloc[0]
            report_dates = df['report_date'].unique().tolist()
            dates_str = ",".join([f"'{d}'" for d in report_dates])
            
            self.conn.execute(f"DELETE FROM {table_name} WHERE symbol = '{symbol}' AND report_date IN ({dates_str})")
            df.to_sql(table_name, self.conn, if_exists='append', index=False)
            
        except Exception as e:
            logger.error(f"存储 {table_name} 失败: {e}")
            raise

    def _check_table_exists(self, table_name: str) -> bool:
        res = self.conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
        return res[0] > 0

    def _validate_schema(self, df: pd.DataFrame, table_name: str):
        """
        列扩展模式：对比新旧列名，发现新列则自动执行 ALTER TABLE。
        """
        res = self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        existing_cols = set([col[1] for col in res])
        current_cols = set(df.columns.tolist())

        # 找出新出现的列
        new_cols = current_cols - existing_cols
        
        if new_cols:
            logger.info(f"表 {table_name} 检测到新财务指标，正在自动扩展列: {new_cols}")
            for col in new_cols:
                # 财务指标统一使用 DOUBLE 类型，symbol/report_date 等除外
                col_type = "VARCHAR" if col in ['symbol', 'report_date', 'ann_date', 'is_audited', '币种', '类型', '数据源'] else "DOUBLE"
                try:
                    self.conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {col_type}')
                except Exception as e:
                    # 如果列已经存在（可能是多线程并发导致），忽略错误
                    if "already exists" in str(e).lower():
                        continue
                    raise
