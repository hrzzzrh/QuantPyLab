from storage.database.view_base import DuckDBView, build_schema_map_expr


class HolderNumberView(DuckDBView):
    name = "holder_number"

    def get_sql(self, warehouse_dir: str) -> str:
        schema_expr = build_schema_map_expr(self.name)
        return rf"""CREATE OR REPLACE VIEW {self.name} AS
            SELECT *, regexp_extract(filename, 'symbol=(\d+)', 1) AS symbol
            FROM read_parquet('{warehouse_dir}/holder_number/*/data.parquet', filename=true, schema={schema_expr})"""
