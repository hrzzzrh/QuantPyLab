from storage.database.view_base import DuckDBView, build_schema_map_expr


class DailyKlineView(DuckDBView):
    name = "daily_kline"

    def get_sql(self, warehouse_dir: str) -> str:
        schema_expr = build_schema_map_expr(self.name)
        return rf"""CREATE OR REPLACE VIEW {self.name} AS
            WITH raw_kline AS (
                SELECT
                    *,
                    regexp_extract(filename, 'symbol=(\d+)', 1) AS partition_symbol
                FROM read_parquet(
                    '{warehouse_dir}/daily_kline/*/data.parquet',
                    filename=true,
                    schema={schema_expr}
                )
            ),
            deduplicated_kline AS (
                SELECT
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    amount,
                    adj_factor,
                    filename,
                    partition_symbol AS symbol
                FROM raw_kline
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY partition_symbol, CAST(date AS DATE)
                    ORDER BY
                        filename,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        amount,
                        adj_factor
                ) = 1
            )
            SELECT * FROM deduplicated_kline"""
