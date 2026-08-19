from pathlib import Path

import pandas as pd

from config.settings import WAREHOUSE_DIR
from storage.database.financial_publish_date_reconciler import (
    PUBLISH_DATE_COLUMN,
    normalize_financial_dates,
)
from storage.file_store.parquet_store import ParquetStore
from utils.logger import logger
from utils.retry import retry


class TTMCalculator:
    """
    财务 TTM (滚动十二个月) 计算器
    公式: TTM = 本期累计 + (上年年报 - 上年同期累计)
    """

    INDICATORS = {
        "net_profit_ttm": (
            "financial_statements/type=income",
            "归属于母公司所有者的净利润",
        ),
        "deduct_net_profit_ttm": ("indicators", "扣非净利润"),
        "revenue_ttm": ("financial_statements/type=income", "营业总收入"),
        "ocf_ttm": ("financial_statements/type=cashflow", "经营活动产生的现金流量净额"),
    }

    def __init__(self):
        self.store = ParquetStore()
        self.warehouse_dir = Path(WAREHOUSE_DIR)
        from storage.database.manager import db_manager

        self.conn = db_manager.get_duckdb_conn()

    def get_existing_report_dates(self) -> set:
        """获取已计算 TTM 的 {symbol}_{report_date} 集合"""
        category = "financial/ttm"
        path = self.store.get_path(category)
        if not any(Path(WAREHOUSE_DIR).glob(f"{category}/*/data.parquet")):
            return set()

        res = self.conn.execute(
            f"SELECT symbol || '_' || report_date FROM read_parquet('{path}', hive_partitioning=1, union_by_name=1)"
        ).fetchall()
        return set([row[0] for row in res])

    def _normalize_pub_date(self, series: pd.Series) -> pd.Series:
        """归一化公告日期为 YYYYMMDD 格式"""
        return normalize_financial_dates(series)

    def _load_data(self, category: str, symbol: str) -> pd.DataFrame:
        path = self.warehouse_dir / category / f"symbol={symbol}" / "data.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    @retry(max_retries=2, delay=1.0)
    def calculate_for_symbol(self, symbol: str):
        """为单只股票计算 TTM 指标"""
        logger.debug(f"开始计算 TTM: {symbol}")

        try:
            # 1. 加载并对齐基础数据
            dfs = []
            required_columns_by_category: dict[str, set[str]] = {}
            for category, col_name in self.INDICATORS.values():
                required_columns_by_category.setdefault(category, set()).add(col_name)

            source_frames: dict[str, tuple[pd.DataFrame, str]] = {}
            for category, required_columns in required_columns_by_category.items():
                df_raw = self._load_data(category, symbol)
                if df_raw.empty or "report_date" not in df_raw.columns:
                    logger.debug(f"跳过缺失数据源: {category}")
                    continue

                if PUBLISH_DATE_COLUMN not in df_raw.columns:
                    logger.debug(f"跳过缺少公告日期列的数据源: {category}")
                    continue
                date_column = PUBLISH_DATE_COLUMN
                available_columns = [
                    column
                    for column in sorted(required_columns)
                    if column in df_raw.columns
                ]
                if not available_columns:
                    logger.debug(f"跳过缺失列: {category} -> {required_columns}")
                    continue

                # 同一财务源表先统一选定修订版本，再供该表的所有 TTM 指标复用。
                # 这样净利润、收入等指标不会从不同修订记录拼接成混合版本。
                source_frame = df_raw[
                    ["report_date", date_column, *available_columns]
                ].copy()
                source_frame["report_date"] = normalize_financial_dates(
                    source_frame["report_date"]
                )
                source_frame[date_column] = self._normalize_pub_date(
                    source_frame[date_column]
                )
                source_frame["_record_tie_breaker"] = pd.util.hash_pandas_object(
                    source_frame[["report_date", date_column, *available_columns]],
                    index=False,
                ).astype("uint64")
                source_frame = (
                    source_frame.sort_values(
                        [date_column, "_record_tie_breaker"],
                        ascending=[True, True],
                        na_position="first",
                        kind="mergesort",
                    )
                    .drop_duplicates("report_date", keep="last")
                    .drop(columns="_record_tie_breaker")
                )
                source_frames[category] = (source_frame, date_column)

            for key, (category, col_name) in self.INDICATORS.items():
                source_info = source_frames.get(category)
                if source_info is None or col_name not in source_info[0].columns:
                    logger.debug(f"跳过缺失列: {category} -> {col_name}")
                    continue

                source_frame, date_column = source_info
                value_column = key.replace("_ttm", "")
                date_output_column = f"pub_date_{key}"
                df_subset = source_frame[["report_date", date_column, col_name]].copy()
                df_subset.rename(
                    columns={
                        col_name: value_column,
                        date_column: date_output_column,
                    },
                    inplace=True,
                )
                dfs.append(df_subset)

            if not dfs:
                logger.warning(f"无足够财务数据，跳过 TTM 计算: {symbol}")
                return

            # 合并所有表 (以 report_date 为准)
            df_base = dfs[0]
            for df in dfs[1:]:
                df_base = pd.merge(df_base, df, on="report_date", how="outer")

            if df_base.empty:
                return

            # 确定最终的披露日期 (取各表中最晚的一个)
            pub_date_cols = [c for c in df_base.columns if c.startswith("pub_date_")]
            df_base["pub_date"] = df_base[pub_date_cols].max(axis=1)

            # 2. 准备偏移列用于计算
            # report_date 格式为 YYYYMMDD (str)
            df_base = df_base.sort_values("report_date")
            df_base["year"] = df_base["report_date"].str[:4].astype(int)
            df_base["period"] = df_base["report_date"].str[4:]

            df_base["last_year_end"] = (df_base["year"] - 1).astype(str) + "1231"
            df_base["last_year_same"] = (df_base["year"] - 1).astype(str) + df_base[
                "period"
            ]

            # 3. 自关联获取上年数据
            target_cols = [
                k.replace("_ttm", "")
                for k in self.INDICATORS.keys()
                if k.replace("_ttm", "") in df_base.columns
            ]
            # 准备“上年终值”查找表
            df_year_end = df_base[df_base["period"] == "1231"][
                ["report_date"] + target_cols
            ].copy()
            year_end_columns = target_cols
            df_year_end.columns = ["last_year_end"] + [
                f"{c}_lye" for c in year_end_columns
            ]

            # 准备“上年同期”查找表
            df_year_same = df_base[["report_date"] + target_cols].copy()
            df_year_same.columns = ["last_year_same"] + [
                f"{c}_lys" for c in year_end_columns
            ]

            # 执行关联
            df_ttm = pd.merge(df_base, df_year_end, on="last_year_end", how="left")
            df_ttm = pd.merge(df_ttm, df_year_same, on="last_year_same", how="left")

            # 4. 计算 TTM
            # 公式: TTM = Current + (LYE - LYS)
            calculated_ttm_cols = []
            for col in target_cols:
                ttm_col = f"{col}_ttm"
                # 只有当 LYE 和 LYS 都不为空时才能计算
                df_ttm[ttm_col] = df_ttm[col] + (
                    df_ttm[f"{col}_lye"] - df_ttm[f"{col}_lys"]
                )
                calculated_ttm_cols.append(ttm_col)

            # 5. 清洗结果并保存
            # TTM 的生效日取当前报告期的统一公告日期。
            # 上年年末和上年同期仅参与数值计算，不再把其日期传播到当前 TTM。
            df_ttm["pub_date"] = self._normalize_pub_date(df_ttm["pub_date"])
            final_cols = ["report_date", "pub_date"] + calculated_ttm_cols
            df_result = df_ttm[final_cols].copy()
            df_result.dropna(subset=calculated_ttm_cols, how="all", inplace=True)

            if not df_result.empty:
                self.store.save_partition(df_result, "financial/ttm", symbol)
                logger.debug(f"TTM 计算完成并保存: {symbol} ({len(df_result)} 条记录)")

        except Exception as e:
            raise e


if __name__ == "__main__":
    # 仅供快速验证
    calculator = TTMCalculator()
    # 可以通过命令行参数指定，或者默认计算茅台
    import sys

    test_symbol = sys.argv[1] if len(sys.argv) > 1 else "600519"
    calculator.calculate_for_symbol(test_symbol)
