from storage.database.view_base import DuckDBView, build_schema_map_expr

class ShareCapitalView(DuckDBView):
    name = "share_capital"
    
    def get_sql(self, warehouse_dir: str) -> str:
        schema_expr = build_schema_map_expr(self.name)
        return f"""CREATE OR REPLACE VIEW {self.name} AS
            SELECT *, regexp_extract(filename, 'symbol=(\d+)', 1) AS symbol
            FROM read_parquet('{warehouse_dir}/share_capital/*/*.parquet', filename=true, schema={schema_expr})"""
