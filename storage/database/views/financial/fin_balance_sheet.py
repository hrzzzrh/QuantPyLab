from storage.database.view_base import DuckDBView, build_schema_map_expr


class BalanceSheetView(DuckDBView):
    name = "fin_balance_sheet"

    def get_sql(self, warehouse_dir: str) -> str:
        schema_expr = build_schema_map_expr(self.name)
        return rf"""CREATE OR REPLACE VIEW {self.name} AS
            SELECT *, regexp_extract(filename, 'symbol=(\d+)', 1) AS symbol
            FROM read_parquet('{warehouse_dir}/financial_statements/type=balance/*/*.parquet', filename=true, schema={schema_expr})"""
