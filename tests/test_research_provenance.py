import sqlite3

import pytest

from backtest.research_provenance import (
    ResearchDataSnapshotChangedError,
    build_research_data_snapshot,
    verify_research_data_snapshot_unchanged,
)


class FakeDatabaseManager:
    def __init__(self, sqlite_path, warehouse_dir):
        self.sqlite_path = sqlite_path
        self.warehouse_dir = warehouse_dir
        self.connection = sqlite3.connect(sqlite_path)
        self.connection.execute(
            """
            CREATE TABLE sync_status (
                dataset TEXT NOT NULL,
                symbol TEXT NOT NULL,
                last_sync_date DATE NOT NULL,
                updated_at DATETIME
            )
            """
        )
        self.connection.execute(
            "INSERT INTO sync_status VALUES (?, ?, ?, ?)",
            ("financial_statements", "000001", "2026-08-21", "2026-08-21"),
        )
        self.connection.commit()

    def get_sqlite_conn(self):
        return self.connection


def test_build_research_data_snapshot_uses_metadata_and_known_root_stats(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    (warehouse_dir / "financial").mkdir(parents=True)
    (warehouse_dir / "daily_kline").mkdir()
    manager = FakeDatabaseManager(tmp_path / "metadata.db", warehouse_dir)

    snapshot = build_research_data_snapshot(manager)

    assert snapshot["status"] == "best_effort"
    assert snapshot["content_addressed"] is False
    assert str(snapshot["snapshot_id"]).startswith("data-")
    assert snapshot["metadata_db_sha256"]
    assert snapshot["sync_status_sha256"]
    assert snapshot["sync_status_row_count"] == 1
    assert snapshot["warehouse_roots"]["financial"]["exists"] is True
    assert snapshot["warehouse_roots"]["daily_kline"]["exists"] is True
    assert snapshot["sync_status"][0]["dataset"] == "financial_statements"

    verified = verify_research_data_snapshot_unchanged(snapshot, dict(snapshot))

    assert verified["verified_unchanged_during_run"] is True
    assert verified["end_snapshot_id"] == snapshot["snapshot_id"]


def test_verify_research_data_snapshot_rejects_observable_sync_change(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    warehouse_dir.mkdir()
    manager = FakeDatabaseManager(tmp_path / "metadata.db", warehouse_dir)
    start_snapshot = build_research_data_snapshot(manager)
    manager.connection.execute(
        "UPDATE sync_status SET last_sync_date = ? WHERE symbol = ?",
        ("2026-08-22", "000001"),
    )
    manager.connection.commit()
    end_snapshot = build_research_data_snapshot(manager)

    with pytest.raises(
        ResearchDataSnapshotChangedError,
        match="研究评估期间数据快照发生变化",
    ):
        verify_research_data_snapshot_unchanged(start_snapshot, end_snapshot)


def test_build_research_data_snapshot_marks_missing_metadata_unavailable():
    snapshot = build_research_data_snapshot(object())

    assert snapshot == {
        "snapshot_id": "unavailable",
        "status": "unavailable",
        "reason": "metadata.db 不存在或数据库管理器未提供 sqlite_path",
    }
