import sqlite3

import duckdb

from storage.database.views.market.stocks import StocksView


def test_stocks_view_exposes_sqlite_registry_through_duckdb(tmp_path):
    metadata_path = tmp_path / "metadata.db"
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir()
    sqlite_connection = sqlite3.connect(metadata_path)
    sqlite_connection.execute(
        """
        CREATE TABLE stocks (
            symbol TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            area TEXT,
            industry TEXT,
            list_date TEXT,
            is_active INTEGER,
            last_trade_date TEXT,
            updated_at DATETIME
        )
        """
    )
    sqlite_connection.execute(
        """
        INSERT INTO stocks VALUES (
            'sz000001', '000001', '平安银行', '深圳', '银行', '19910403',
            0, '20240103', '2024-01-04 09:30:00'
        )
        """
    )
    sqlite_connection.commit()
    sqlite_connection.close()

    duckdb_connection = duckdb.connect()
    try:
        duckdb_connection.execute(StocksView().get_sql(str(warehouse_path)))
        row = duckdb_connection.execute(
            """
            SELECT code, is_active, last_trade_date
            FROM stocks
            """
        ).fetchone()
    finally:
        duckdb_connection.close()

    assert row == ("000001", 0, "20240103")
