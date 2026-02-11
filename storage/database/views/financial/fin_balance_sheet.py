from storage.database.view_base import DuckDBView

class BalanceSheetView(DuckDBView):
    name = "fin_balance_sheet"
    
    def get_sql(self, warehouse_dir: str) -> str:
        return f"CREATE OR REPLACE VIEW {self.name} AS SELECT * FROM read_parquet('{warehouse_dir}/financial_statements/type=balance/*/*.parquet', hive_partitioning=1, union_by_name=1)"
