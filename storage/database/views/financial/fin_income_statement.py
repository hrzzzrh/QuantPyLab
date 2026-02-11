from storage.database.view_base import DuckDBView

class IncomeStatementView(DuckDBView):
    name = "fin_income_statement"
    
    def get_sql(self, warehouse_dir: str) -> str:
        return f"CREATE OR REPLACE VIEW {self.name} AS SELECT * FROM read_parquet('{warehouse_dir}/financial_statements/type=income/*/*.parquet', hive_partitioning=1, union_by_name=1)"
