import pandas as pd
import numpy as np
from pathlib import Path
from storage.file_store.parquet_store import ParquetStore
from utils.logger import logger
from config.settings import WAREHOUSE_DIR

class TTMCalculator:
    """
    财务 TTM (滚动十二个月) 计算器
    公式: TTM = 本期累计 + (上年年报 - 上年同期累计)
    """

    INDICATORS = {
        'net_profit_ttm': ('financial_statements/type=income', '归属于母公司所有者的净利润'),
        'deduct_net_profit_ttm': ('indicators', '扣非净利润'),
        'revenue_ttm': ('financial_statements/type=income', '营业总收入'),
        'ocf_ttm': ('financial_statements/type=cashflow', '经营活动产生的现金流量净额'),
    }

    def __init__(self):
        self.store = ParquetStore()
        self.warehouse_dir = Path(WAREHOUSE_DIR)
        from storage.database.manager import db_manager
        self.conn = db_manager.get_duckdb_conn()

    def get_existing_report_dates(self) -> set:
        """获取已计算 TTM 的 {symbol}_{report_date} 集合"""
        category = 'financial/ttm'
        path = self.store.get_path(category)
        if not any(Path(WAREHOUSE_DIR).glob(f"{category}/*/data.parquet")):
            return set()
            
        res = self.conn.execute(f"SELECT symbol || '_' || report_date FROM read_parquet('{path}', hive_partitioning=1, union_by_name=1)").fetchall()
        return set([row[0] for row in res])

    def _normalize_pub_date(self, series: pd.Series) -> pd.Series:
        """归一化公告日期为 YYYYMMDD 格式"""
        # 处理 YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD
        series = series.astype(str).str.replace(r'[- :]', '', regex=True).str[:8]
        return series

    def _load_data(self, category: str, symbol: str) -> pd.DataFrame:
        path = self.warehouse_dir / category / f"symbol={symbol}" / "data.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def calculate_for_symbol(self, symbol: str):
        """为单只股票计算 TTM 指标"""
        logger.info(f"开始计算 TTM: {symbol}")
        
        # 1. 加载并对齐基础数据
        dfs = []
        for key, (category, col_name) in self.INDICATORS.items():
            df_raw = self._load_data(category, symbol)
            if df_raw.empty or col_name not in df_raw.columns:
                logger.debug(f"跳过缺失列: {category} -> {col_name}")
                continue
            
            # 只保留核心列并归一化日期
            df_subset = df_raw[['report_date', '公告日期', col_name]].copy()
            df_subset['公告日期'] = self._normalize_pub_date(df_subset['公告日期'])
            df_subset.rename(columns={col_name: key.replace('_ttm', ''), '公告日期': f'pub_date_{key}'}, inplace=True)
            
            # 确保每个 report_date 只有一行
            df_subset = df_subset.sort_values('pub_date_' + key).drop_duplicates('report_date', keep='last')
            dfs.append(df_subset)

        if not dfs:
            logger.warning(f"无足够财务数据，跳过 TTM 计算: {symbol}")
            return

        # 合并所有表 (以 report_date 为准)
        df_base = dfs[0]
        for df in dfs[1:]:
            df_base = pd.merge(df_base, df, on='report_date', how='outer')

        if df_base.empty:
            return

        # 确定最终的披露日期 (取各表中最晚的一个)
        pub_date_cols = [c for c in df_base.columns if c.startswith('pub_date_')]
        df_base['pub_date'] = df_base[pub_date_cols].max(axis=1)
        
        # 2. 准备偏移列用于计算
        # report_date 格式为 YYYYMMDD (str)
        df_base = df_base.sort_values('report_date')
        df_base['year'] = df_base['report_date'].str[:4].astype(int)
        df_base['period'] = df_base['report_date'].str[4:]
        
        df_base['last_year_end'] = (df_base['year'] - 1).astype(str) + "1231"
        df_base['last_year_same'] = (df_base['year'] - 1).astype(str) + df_base['period']

        # 3. 自关联获取上年数据
        target_cols = [k.replace('_ttm', '') for k in self.INDICATORS.keys() if k.replace('_ttm', '') in df_base.columns]
        
        # 准备“上年终值”查找表
        df_year_end = df_base[df_base['period'] == '1231'][['report_date'] + target_cols].copy()
        df_year_end.columns = ['last_year_end'] + [f"{c}_lye" for c in target_cols]
        
        # 准备“上年同期”查找表
        df_year_same = df_base[['report_date'] + target_cols].copy()
        df_year_same.columns = ['last_year_same'] + [f"{c}_lys" for c in target_cols]

        # 执行关联
        df_ttm = pd.merge(df_base, df_year_end, on='last_year_end', how='left')
        df_ttm = pd.merge(df_ttm, df_year_same, on='last_year_same', how='left')

        # 4. 计算 TTM
        # 公式: TTM = Current + (LYE - LYS)
        calculated_ttm_cols = []
        for col in target_cols:
            ttm_col = f"{col}_ttm"
            # 只有当 LYE 和 LYS 都不为空时才能计算
            df_ttm[ttm_col] = df_ttm[col] + (df_ttm[f"{col}_lye"] - df_ttm[f"{col}_lys"])
            calculated_ttm_cols.append(ttm_col)

        # 5. 清洗结果并保存
        final_cols = ['report_date', 'pub_date'] + calculated_ttm_cols
        df_result = df_ttm[final_cols].copy()
        df_result.dropna(subset=calculated_ttm_cols, how='all', inplace=True)
        
        if not df_result.empty:
            self.store.save_partition(df_result, 'financial/ttm', symbol)
            logger.info(f"TTM 计算完成并保存: {symbol} ({len(df_result)} 条记录)")

if __name__ == "__main__":
    # 仅供快速验证
    calculator = TTMCalculator()
    # 可以通过命令行参数指定，或者默认计算茅台
    import sys
    test_symbol = sys.argv[1] if len(sys.argv) > 1 else "600519"
    calculator.calculate_for_symbol(test_symbol)
