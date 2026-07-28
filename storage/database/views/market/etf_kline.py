from storage.database.view_base import DuckDBView

class ETFKlineView(DuckDBView):
    name = "etf_kline"
    
    def get_sql(self, warehouse_dir: str) -> str:
        return f"CREATE OR REPLACE VIEW {self.name} AS SELECT * FROM read_parquet('{warehouse_dir}/etf_kline/*/*.parquet', hive_partitioning=1, union_by_name=1)"
