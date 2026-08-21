from storage.database.view_base import DuckDBView, build_schema_map_expr


class DailyKlineRawView(DuckDBView):
    """暴露原始行情行，供调用方按需执行轻量日期区间查询。"""

    name = "daily_kline_raw"

    def get_sql(self, warehouse_dir: str) -> str:
        schema_expr = build_schema_map_expr("daily_kline")
        return rf"""CREATE OR REPLACE VIEW {self.name} AS
            SELECT
                CAST(date AS DATE) AS date,
                open,
                high,
                low,
                close,
                volume,
                amount,
                adj_factor,
                filename,
                regexp_extract(filename, 'symbol=(\d+)', 1) AS symbol
            FROM read_parquet(
                '{warehouse_dir}/daily_kline/*/data.parquet',
                filename=true,
                schema={schema_expr}
            )"""
