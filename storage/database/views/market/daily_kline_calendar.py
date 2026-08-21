from storage.database.view_base import DuckDBView, build_schema_map_expr


class DailyKlineCalendarView(DuckDBView):
    """只暴露日期和分区股票代码，供回测日历查询使用。"""

    name = "daily_kline_calendar"

    def get_sql(self, warehouse_dir: str) -> str:
        schema_expr = build_schema_map_expr("daily_kline")
        return rf"""CREATE OR REPLACE VIEW {self.name} AS
            SELECT
                CAST(date AS DATE) AS date,
                regexp_extract(filename, 'symbol=(\d+)', 1) AS symbol
            FROM read_parquet(
                '{warehouse_dir}/daily_kline/*/data.parquet',
                filename=true,
                schema={schema_expr}
            )"""
