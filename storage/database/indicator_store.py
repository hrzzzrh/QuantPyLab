import pandas as pd
from utils.logger import logger
from storage.database.manager import db_manager

class IndicatorStore:
    """
    财务指标存储器 (Analysis Layer)
    负责将东财的财务指标数据 (中文列名) 存入 DuckDB。
    """

    def __init__(self):
        self.conn = db_manager.get_duckdb_conn()
        self.table_name = "fin_indicator"

    def save_indicators(self, df: pd.DataFrame):
        """
        保存指标数据，支持动态列扩展。
        df 必须已经是中文列名。
        """
        if df.empty:
            return

        try:
            # 1. 检查表是否存在
            if not self._check_table_exists():
                logger.info(f"创建新表 {self.table_name}...")
                # 强制指定关键列类型，其他列自动推断
                # symbol: VARCHAR, report_date: VARCHAR
                df.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
                # 为 symbol 和 report_date 创建索引 (DuckDB 自动优化，但显式索引更好)
                # 注意：DuckDB 的 PRIMARY KEY 支持有限，但在分析场景下我们用逻辑去重
                return

            # 2. 动态列扩展 (Schema Evolution)
            self._validate_and_evolve_schema(df)

            # 3. 逻辑 Upsert (先删后增)
            # 这种模式在列式数据库中比 UPDATE 更高效
            symbol = df['symbol'].iloc[0]
            report_dates = df['report_date'].unique().tolist()
            
            # 安全地构造 SQL 列表字符串
            dates_str = ",".join([f"'{d}'" for d in report_dates])
            
            self.conn.execute(f"""
                DELETE FROM {self.table_name} 
                WHERE symbol = '{symbol}' 
                AND report_date IN ({dates_str})
            """)
            
            df.to_sql(self.table_name, self.conn, if_exists='append', index=False)
            logger.info(f"成功入库 {symbol}: {len(df)} 条指标记录")
            
        except Exception as e:
            logger.error(f"存储财务指标失败: {e}")
            raise

    def _check_table_exists(self) -> bool:
        res = self.conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{self.table_name}'").fetchone()
        return res[0] > 0

    def _validate_and_evolve_schema(self, df: pd.DataFrame):
        """对比新旧列名，自动 ALTER TABLE 添加新指标"""
        res = self.conn.execute(f"PRAGMA table_info('{self.table_name}')").fetchall()
        existing_cols = set([col[1] for col in res])
        current_cols = set(df.columns.tolist())

        new_cols = current_cols - existing_cols
        
        if new_cols:
            logger.info(f"[{self.table_name}] 发现新指标，正在扩展 Schema: {new_cols}")
            for col in new_cols:
                # 财务指标统一使用 DOUBLE，文本类字段除外
                col_type = "VARCHAR" if col in ['symbol', 'report_date', 'ann_date', '公告日期', '报告期名称'] else "DOUBLE"
                try:
                    # 列名可能包含特殊字符，需用引号包裹
                    self.conn.execute(f'ALTER TABLE {self.table_name} ADD COLUMN "{col}" {col_type}')
                except Exception as e:
                    if "already exists" in str(e).lower(): continue
                    logger.warning(f"添加列 {col} 失败: {e}")

    def get_existing_report_dates(self) -> set:
        """获取数据库中已有的 {symbol}_{report_date} 集合"""
        if not self._check_table_exists():
            return set()
        res = self.conn.execute(f"SELECT symbol || '_' || report_date FROM {self.table_name}").fetchall()
        return set([row[0] for row in res])

    def get_stocks_without_indicators(self, all_codes: list) -> list:
        """从全量列表中筛选出在数据库中完全没有任何指标记录的股票"""
        if not self._check_table_exists():
            return all_codes
        
        # 查找有记录的 symbol (对应 stocks 表的 code)
        existing_symbols = self.conn.execute(f"SELECT DISTINCT symbol FROM {self.table_name}").fetchall()
        existing_symbols_set = set([row[0] for row in existing_symbols])
        
        return [c for c in all_codes if c not in existing_symbols_set]

    def get_existing_dates(self, symbol: str) -> set:
        """获取某只股票已有的报告期列表，用于增量判断"""
        if not self._check_table_exists():
            return set()
        
        res = self.conn.execute(f"SELECT report_date FROM {self.table_name} WHERE symbol = '{symbol}'").fetchall()
        return set([row[0] for row in res])
