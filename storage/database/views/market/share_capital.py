from storage.database.view_base import DuckDBView

class ShareCapitalView(DuckDBView):
    name = "share_capital"
    
    def get_sql(self, warehouse_dir: str) -> str:
        return f"CREATE OR REPLACE VIEW {self.name} AS SELECT * FROM read_parquet('{warehouse_dir}/share_capital/*/*.parquet', hive_partitioning=1, union_by_name=1)"
