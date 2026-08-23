from pathlib import Path

from storage.database.view_base import DuckDBView


class StocksView(DuckDBView):
    """Expose the SQLite stock registry through the unified DuckDB view layer."""

    name = "stocks"

    def get_sql(self, warehouse_dir: str) -> str:
        metadata_db_path = (Path(warehouse_dir).parent / "metadata.db").resolve()
        escaped_path = str(metadata_db_path).replace("'", "''")
        return rf"""CREATE OR REPLACE VIEW {self.name} AS
            SELECT
                CAST(symbol AS VARCHAR) AS symbol,
                CAST(code AS VARCHAR) AS code,
                CAST(name AS VARCHAR) AS name,
                CAST(area AS VARCHAR) AS area,
                CAST(industry AS VARCHAR) AS industry,
                CAST(list_date AS VARCHAR) AS list_date,
                CAST(is_active AS INTEGER) AS is_active,
                CAST(last_trade_date AS VARCHAR) AS last_trade_date,
                CAST(updated_at AS TIMESTAMP) AS updated_at
            FROM sqlite_scan('{escaped_path}', 'stocks')"""
