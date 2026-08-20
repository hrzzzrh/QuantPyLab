"""Unit tests for manifest-backed KLC staging and migration."""

import base64
import csv
import json
import os
import subprocess
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

from storage.database import manager as manager_mod
from tools import kline_source_migration as migration_mod
from utils.requests_protection import SinaBlockedError
from utils.tencent_kline import TencentKlineFetchError


@pytest.fixture(autouse=True)
def disable_migration_sleep(monkeypatch):
    monkeypatch.setattr(migration_mod.time, "sleep", lambda _seconds: None)


@pytest.fixture
def isolated_metadata(tmp_path, monkeypatch):
    sqlite_path = tmp_path / "metadata.db"
    original_sqlite_path = manager_mod.db_manager.sqlite_path
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    manager_mod.db_manager._sqlite_conn = None
    manager_mod.db_manager.sqlite_path = sqlite_path
    manager_mod.db_manager.initialize_schema()
    conn = manager_mod.db_manager.get_sqlite_conn()
    yield conn
    manager_mod.db_manager._sqlite_conn = None
    manager_mod.db_manager.sqlite_path = original_sqlite_path


def make_frame(
    dates=("2026-08-03", "2026-08-04"),
    close=(10.0, 11.0),
    factor=(2.0, 2.0),
    close_hfq=None,
):
    frame = pd.DataFrame(
        {
            "date": list(dates),
            "open": [value - 0.5 for value in close],
            "high": [value + 0.5 for value in close],
            "low": [value - 1.0 for value in close],
            "close": list(close),
            "volume": [100.0, 200.0],
            "amount": [1000.0, 2200.0],
            "adj_factor": list(factor),
        }
    )
    if close_hfq is not None:
        frame["close_hfq"] = list(close_hfq)
    return frame


def make_quality_frame(**kwargs):
    frame = make_frame(**kwargs)
    frame["close_hfq"] = frame["close"] * frame["adj_factor"]
    return frame


def insert_stock(conn, symbol, active=1):
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES (?, ?, ?, ?)",
        (symbol, symbol, f"name-{symbol}", active),
    )
    conn.commit()


def write_canonical(warehouse_dir: Path, symbol: str, frame: pd.DataFrame):
    path = warehouse_dir / "daily_kline" / f"symbol={symbol}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def downgrade_manifest(path: Path, removed_fields: set[str]) -> None:
    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        fieldnames = [
            field for field in reader.fieldnames or [] if field not in removed_fields
        ]
        rows = list(reader)
    buffer = StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fieldnames})
    path.write_text(buffer.getvalue(), encoding="utf-8")


def downgrade_metadata(path: Path, version: int) -> None:
    metadata = json.loads(path.read_text())
    metadata["manifest_schema_version"] = version
    if version != migration_mod.MANIFEST_SCHEMA_VERSION:
        metadata.pop("target_count", None)
        metadata.pop("target_symbols_sha256", None)
    migration_mod._write_json(path, metadata)


def test_select_migration_symbols_includes_delisted_and_samples_groups(
    isolated_metadata,
):
    symbols = (
        "600009",
        "600010",
        "600011",
        "600012",
        "000011",
        "000012",
        "000013",
        "000014",
        "002005",
        "002006",
        "002007",
        "300008",
        "300009",
        "300010",
        "688001",
        "688002",
        "688003",
        "920001",
        "920002",
        "920003",
    )
    for symbol in symbols:
        insert_stock(isolated_metadata, symbol)
    insert_stock(isolated_metadata, "600099", active=0)
    isolated_metadata.execute(
        "UPDATE stocks SET last_trade_date='20091215' WHERE symbol='600099'"
    )
    isolated_metadata.commit()

    selected = migration_mod.select_migration_symbols(limit=20, conn=isolated_metadata)
    selected_symbols = [target.symbol for target in selected]

    assert len(selected_symbols) == 20
    assert set(migration_mod.PRIORITY_SYMBOLS).issubset(selected_symbols)
    assert "600099" not in selected_symbols
    assert {symbol[:3] for symbol in selected_symbols} == set(
        migration_mod.SAMPLE_GROUPS
    )
    all_targets = migration_mod.select_migration_symbols(
        limit=None, conn=isolated_metadata
    )
    assert "600099" not in {target.symbol for target in all_targets}


def test_select_migration_symbols_rejects_pre_cutoff_delisted(
    isolated_metadata,
):
    insert_stock(isolated_metadata, "000003", active=0)
    isolated_metadata.execute(
        "UPDATE stocks SET last_trade_date='20020426' WHERE symbol='000003'"
    )
    isolated_metadata.commit()

    with pytest.raises(migration_mod.MigrationSelectionError, match="范围内无 K 线"):
        migration_mod.select_migration_symbols(symbol="000003", conn=isolated_metadata)


def test_validate_frame_rejects_duplicate_dates_and_bad_factor():
    duplicate = make_frame(dates=("2026-08-03", "2026-08-03"))
    with pytest.raises(migration_mod.MigrationValidationError, match="重复"):
        migration_mod._validate_frame(duplicate)

    invalid_factor = make_frame(factor=(0.0, 2.0))
    with pytest.raises(migration_mod.MigrationValidationError, match="大于 0"):
        migration_mod._validate_frame(invalid_factor)

    invalid_date = make_frame(dates=("", "2026-08-04"))
    with pytest.raises(migration_mod.MigrationValidationError, match="无效日期"):
        migration_mod._validate_frame(invalid_date)

    unsorted = make_frame(dates=("2026-08-04", "2026-08-03"))
    with pytest.raises(migration_mod.MigrationValidationError, match="升序"):
        migration_mod._validate_frame(unsorted)


def test_validate_frame_rejects_invalid_ohlc_and_weekend_quality():
    invalid_ohlc = make_frame()
    invalid_ohlc.loc[0, "high"] = invalid_ohlc.loc[0, "low"] - 1
    with pytest.raises(migration_mod.MigrationValidationError, match="最高价"):
        migration_mod._validate_frame(invalid_ohlc)

    weekend = make_frame(dates=("2026-08-08", "2026-08-10"))
    normalized = migration_mod._validate_frame(weekend)
    assert migration_mod._quality_metrics(normalized)["weekend_rows"] == 1


def test_validate_kline_frame_rejects_weekend_and_hfq_mismatch():
    weekend = make_quality_frame(dates=("2026-08-08", "2026-08-10"))
    with pytest.raises(migration_mod.MigrationQualityError, match="周末"):
        migration_mod.validate_kline_frame(weekend)

    mismatch = make_quality_frame()
    mismatch.loc[0, "close_hfq"] += 1
    with pytest.raises(migration_mod.MigrationQualityError, match="close_hfq"):
        migration_mod.validate_kline_frame(mismatch)


def test_build_run_failure_does_not_leave_partial_run(tmp_path, monkeypatch):
    root = tmp_path / "migration"
    root.mkdir()
    targets = [migration_mod.MigrationTarget("600009", "name-600009")]

    def fail_manifest(*args, **kwargs):
        raise OSError("manifest write failed")

    monkeypatch.setattr(migration_mod.MigrationManifest, "create", fail_manifest)

    with pytest.raises(OSError, match="manifest write failed"):
        migration_mod._build_run(
            root,
            "atomic-run",
            targets,
            "sina-klc",
            "20100101",
            "20260807",
            False,
        )

    assert not (root / "atomic-run").exists()
    assert list(root.iterdir()) == []


def test_select_migration_symbols_rejects_non_numeric_symbol(isolated_metadata):
    with pytest.raises(migration_mod.MigrationSelectionError, match="6 位数字"):
        migration_mod.select_migration_symbols(
            symbol="../../escape", conn=isolated_metadata
        )


def test_stage_only_writes_staging_and_preserves_canonical(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    old_frame = make_frame(factor=(1.0, 1.0), close_hfq=(10.0, 11.0))
    canonical_path = write_canonical(warehouse_dir, symbol, old_frame)

    calls = []

    def fetcher(sina_symbol, start_date, end_date):
        calls.append((sina_symbol, start_date, end_date))
        return make_quality_frame(factor=(2.0, 2.0))

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        stage_only=True,
        warehouse_dir=warehouse_dir,
        fetcher=fetcher,
    )

    assert result.failed_symbols == ()
    assert result.staged_symbols == (symbol,)
    assert calls == [("sh600009", "20100101", "20260807")]
    pd.testing.assert_frame_equal(pd.read_parquet(canonical_path), old_frame)

    metadata = json.loads(
        (result.run_dir / migration_mod.METADATA_FILENAME).read_text()
    )
    assert metadata["target_count"] == 1
    assert metadata["target_symbols_sha256"] == migration_mod._target_symbols_sha256(
        [symbol]
    )

    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert row["status"] == "staged"
    assert row["source_used"] == "sina-klc"
    assert Path(row["stage_path"]).exists()
    assert row["weekend_rows"] == "0"
    assert row["weekend_rows_filtered"] == "0"
    assert row["known_bad_rows_filtered"] == "0"
    assert row["hfq_relation_mismatch_count"] == "0"
    assert row["hfq_source_rows"] == "0"
    assert row["hfq_forward_filled_rows"] == "0"


def test_manifest_rejects_header_only_file(tmp_path):
    path = tmp_path / "manifest.csv"
    path.write_text(",".join(migration_mod.MANIFEST_FIELDS) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="不得为空"):
        migration_mod.MigrationManifest.load(path)


def test_resume_rejects_truncated_manifest(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    insert_stock(isolated_metadata, "600010")
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        limit=None,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    lines = manifest_path.read_text(encoding="utf-8").splitlines()
    manifest_path.write_text("\n".join(lines[:2]) + "\n", encoding="utf-8")

    with pytest.raises(migration_mod.MigrationValidationError, match="股票数量"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=tmp_path / "warehouse",
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_migration_clamps_start_date_to_minimum(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    calls = []

    def fetcher(sina_symbol, start_date, end_date):
        calls.append((sina_symbol, start_date, end_date))
        return make_quality_frame()

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        start_date="19900101",
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=fetcher,
    )

    metadata = json.loads(
        (result.run_dir / migration_mod.METADATA_FILENAME).read_text()
    )
    assert calls == [("sh600009", "20100101", "20260807")]
    assert metadata["start_date"] == "20100101"


def test_quality_metrics_record_zero_values_and_factor_range():
    frame = make_frame(factor=(1.0, 2.0), close_hfq=(10.0, 22.0))
    frame.loc[1, "volume"] = 0
    frame.loc[1, "amount"] = 0
    metrics = migration_mod._quality_metrics(
        migration_mod._validate_frame(frame, require_close_hfq=True)
    )
    assert metrics["zero_volume_rows"] == 1
    assert metrics["zero_amount_rows"] == 1
    assert metrics["factor_one_rows"] == 1
    assert metrics["factor_min"] == 1.0
    assert metrics["factor_max"] == 2.0


def test_quality_metrics_record_filtered_weekend_rows():
    frame = make_quality_frame()
    frame.attrs["weekend_rows_filtered"] = 3

    metrics = migration_mod._quality_metrics(
        migration_mod._validate_frame(frame, require_close_hfq=True)
    )

    assert metrics["weekend_rows_filtered"] == 3


def test_quality_metrics_record_known_bad_rows():
    frame = make_quality_frame()
    frame.attrs["known_bad_rows_filtered"] = 1

    metrics = migration_mod._quality_metrics(
        migration_mod._validate_frame(frame, require_close_hfq=True)
    )

    assert metrics["known_bad_rows_filtered"] == 1


def test_completed_staging_allows_next_sample_run(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    insert_stock(isolated_metadata, "600010")
    warehouse_dir = tmp_path / "warehouse"

    def fetcher(_sina_symbol, _start_date, _end_date):
        return make_quality_frame(factor=(2.0, 2.0))

    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600009",
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=fetcher,
    )
    second = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600010",
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=fetcher,
    )

    assert first.staged_symbols == ("600009",)
    assert second.staged_symbols == ("600010",)


def test_local_partition_is_ignored_during_staging(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    old_frame = make_frame(factor=(1.0, 1.0), close_hfq=(10.0, 11.0))
    canonical_path = write_canonical(warehouse_dir, symbol, old_frame)

    def fetcher(_sina_symbol, _start_date, _end_date):
        return make_quality_frame(close=(10.5, 11.0), factor=(2.0, 2.0))

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=fetcher,
    )

    assert result.failed_symbols == ()
    assert result.staged_symbols == (symbol,)
    pd.testing.assert_frame_equal(pd.read_parquet(canonical_path), old_frame)
    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert row["status"] == "staged"


def test_staging_requires_close_hfq_and_validates_relation(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)

    missing_hfq = make_frame()
    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "missing-hfq",
        fetcher=lambda *_args: missing_hfq,
    )
    assert result.failed_symbols == (symbol,)
    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert "close_hfq" in row["error_message"]
    assert row["hfq_relation_mismatch_count"] == ""

    bad_relation = make_quality_frame()
    bad_relation.loc[0, "close_hfq"] += 1
    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "bad-relation",
        fetcher=lambda *_args: bad_relation,
    )
    assert result.failed_symbols == (symbol,)
    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert "close_hfq" in row["error_message"]
    assert row["hfq_relation_mismatch_count"] == "1"


def test_quality_failure_preserves_metrics_and_retry_clears_them(
    isolated_metadata, tmp_path
):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    calls = []

    def fetcher(_sina_symbol, _start_date, _end_date):
        calls.append(True)
        if len(calls) == 1:
            return make_quality_frame()
        if len(calls) == 2:
            frame = make_quality_frame()
            frame.loc[0, "close_hfq"] += 1
            return frame
        raise ValueError("ordinary fetch failure")

    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=fetcher,
    )
    first_row = next(migration_mod.iter_manifest_rows(first.run_dir))
    stage_path = Path(first_row["stage_path"])
    stage_path.write_bytes(b"tampered")

    quality_failed = migration_mod.run_kline_source_migration(
        source="sina-klc",
        resume_run_id=first.run_id,
        warehouse_dir=tmp_path / "warehouse",
        fetcher=fetcher,
    )
    quality_row = next(migration_mod.iter_manifest_rows(quality_failed.run_dir))
    assert quality_failed.failed_symbols == (symbol,)
    assert quality_row["stage_path"] == ""
    assert quality_row["hfq_relation_mismatch_count"] == "1"
    assert quality_row["new_rows"] == "2"
    assert not stage_path.exists()

    ordinary_failed = migration_mod.run_kline_source_migration(
        source="sina-klc",
        resume_run_id=first.run_id,
        warehouse_dir=tmp_path / "warehouse",
        fetcher=fetcher,
    )
    ordinary_row = next(migration_mod.iter_manifest_rows(ordinary_failed.run_dir))
    assert ordinary_failed.failed_symbols == (symbol,)
    assert ordinary_row["hfq_relation_mismatch_count"] == ""
    assert ordinary_row["new_rows"] == ""


def test_blocked_retry_clears_stale_staging_evidence(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    row = next(migration_mod.iter_manifest_rows(first.run_dir))
    stage_path = Path(row["stage_path"])
    stage_path.write_bytes(b"tampered")

    resumed = migration_mod.run_kline_source_migration(
        source="sina-klc",
        resume_run_id=first.run_id,
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: (_ for _ in ()).throw(SinaBlockedError("blocked")),
    )
    resumed_row = next(migration_mod.iter_manifest_rows(resumed.run_dir))
    assert resumed.stopped_by_sina_block is True
    assert resumed_row["status"] == "failed"
    assert resumed_row["stage_path"] == ""
    assert resumed_row["stage_sha256"] == ""
    assert resumed_row["new_start_date"] == ""
    assert resumed_row["new_end_date"] == ""
    assert resumed_row["hfq_source_rows"] == ""
    assert not stage_path.exists()


def test_cleanup_failure_keeps_safe_stage_evidence(
    isolated_metadata, tmp_path, monkeypatch
):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    monkeypatch.setattr(migration_mod, "_discard_staged_file", lambda *_args: False)

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )

    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    expected_path = result.run_dir / "staged" / f"symbol={symbol}" / "data.parquet"
    assert result.failed_symbols == (symbol,)
    assert row["cleanup_failed"] == "1"
    assert Path(row["stage_path"]) == expected_path


def test_legacy_v2_run_requires_new_run(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    downgrade_metadata(metadata_path, migration_mod.LEGACY_MANIFEST_SCHEMA_VERSION)
    downgrade_manifest(
        manifest_path,
        {
            "cleanup_failed",
            "source_used",
            "weekend_rows_filtered",
            "known_bad_rows_filtered",
        },
    )

    with pytest.raises(migration_mod.MigrationValidationError, match="不可变"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_previous_manifest_version_requires_new_run(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    downgrade_metadata(metadata_path, migration_mod.PREVIOUS_MANIFEST_SCHEMA_VERSION)
    downgrade_manifest(manifest_path, {"known_bad_rows_filtered"})

    with pytest.raises(migration_mod.MigrationValidationError, match="不可变"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=tmp_path / "warehouse",
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_schema_six_run_without_target_evidence_requires_new_run(
    isolated_metadata, tmp_path
):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    metadata = json.loads(metadata_path.read_text())
    metadata.pop("target_count")
    metadata.pop("target_symbols_sha256")
    migration_mod._write_json(metadata_path, metadata)

    with pytest.raises(migration_mod.MigrationValidationError, match="不可变"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=tmp_path / "warehouse",
            fetcher=lambda *_args: make_quality_frame(),
        )
    with pytest.raises(migration_mod.MigrationValidationError, match="不可变"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            symbol=symbol,
            end_date="20260807",
            warehouse_dir=tmp_path / "warehouse",
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_intermediate_manifest_version_requires_new_run(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    downgrade_metadata(
        metadata_path, migration_mod.INTERMEDIATE_MANIFEST_SCHEMA_VERSION
    )
    downgrade_manifest(
        manifest_path, {"known_bad_rows_filtered", "weekend_rows_filtered"}
    )

    with pytest.raises(migration_mod.MigrationValidationError, match="不可变"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=tmp_path / "warehouse",
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_older_manifest_version_requires_new_run(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    downgrade_metadata(metadata_path, migration_mod.OLDER_MANIFEST_SCHEMA_VERSION)
    downgrade_manifest(
        manifest_path,
        {
            "known_bad_rows_filtered",
            "weekend_rows_filtered",
            "source_used",
        },
    )

    with pytest.raises(migration_mod.MigrationValidationError, match="不可变"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=tmp_path / "warehouse",
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_resume_rejects_pre_cutoff_run_start_date(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    metadata = json.loads(metadata_path.read_text())
    metadata["start_date"] = "19900101"
    migration_mod._write_json(metadata_path, metadata)

    with pytest.raises(ValueError, match="请新建"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=tmp_path / "warehouse",
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_unknown_source_manifest_version_blocks_new_run(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    warehouse_dir = tmp_path / "warehouse"
    root = migration_mod.get_migration_root(warehouse_dir)
    run_dir = root / "future-run"
    run_dir.mkdir(parents=True)
    migration_mod._write_json(
        run_dir / migration_mod.METADATA_FILENAME,
        {
            "run_id": "future-run",
            "manifest_schema_version": 999,
            "source": "sina-klc",
            "dry_run": False,
        },
    )
    migration_mod.MigrationManifest.create(
        run_dir / migration_mod.MANIFEST_FILENAME,
        [migration_mod.MigrationTarget("600009", "name")],
    )

    with pytest.raises(ValueError, match="不支持的迁移 manifest 版本"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            symbol="600009",
            end_date="20260807",
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_unfinished_upgrade_journal_blocks_new_run(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600009",
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    journal_path = first.run_dir / migration_mod.UPGRADE_JOURNAL_FILENAME
    journal_path.write_text(
        json.dumps(
            {
                "run_id": first.run_id,
                "from_version": migration_mod.LEGACY_MANIFEST_SCHEMA_VERSION,
                "to_version": migration_mod.MANIFEST_SCHEMA_VERSION,
            }
        )
    )

    with pytest.raises(migration_mod.MigrationLockError, match="resume/recover"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            symbol="600009",
            end_date="20260807",
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_corrupt_upgrade_journal_does_not_overwrite_current_pair(
    isolated_metadata, tmp_path
):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    current_manifest = manifest_path.read_bytes()
    current_metadata = metadata_path.read_bytes()
    journal = {
        "run_id": first.run_id,
        "from_version": migration_mod.LEGACY_MANIFEST_SCHEMA_VERSION,
        "to_version": migration_mod.MANIFEST_SCHEMA_VERSION,
        "old_manifest_b64": "Yg==",
        "old_metadata_b64": "Yg==",
        "new_manifest_b64": "Yg==",
        "new_metadata_b64": "Yg==",
    }
    journal_path = first.run_dir / migration_mod.UPGRADE_JOURNAL_FILENAME
    journal_path.write_text(json.dumps(journal))

    with pytest.raises(migration_mod.MigrationValidationError):
        migration_mod._recover_manifest_upgrade(first.run_dir)
    assert manifest_path.read_bytes() == current_manifest
    assert metadata_path.read_bytes() == current_metadata
    assert journal_path.exists()


def test_upgrade_journal_rejects_target_digest_mismatch(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    new_manifest_bytes = manifest_path.read_bytes()
    new_metadata = json.loads(metadata_path.read_text())
    downgrade_metadata(metadata_path, migration_mod.PREVIOUS_MANIFEST_SCHEMA_VERSION)
    downgrade_manifest(manifest_path, {"known_bad_rows_filtered"})
    old_manifest_bytes = manifest_path.read_bytes()
    old_metadata_bytes = metadata_path.read_bytes()
    new_metadata["target_symbols_sha256"] = "0" * 64

    migration_mod._write_json(
        first.run_dir / migration_mod.UPGRADE_JOURNAL_FILENAME,
        {
            "run_id": first.run_id,
            "from_version": migration_mod.PREVIOUS_MANIFEST_SCHEMA_VERSION,
            "to_version": migration_mod.MANIFEST_SCHEMA_VERSION,
            "old_manifest_b64": base64.b64encode(old_manifest_bytes).decode("ascii"),
            "old_metadata_b64": base64.b64encode(old_metadata_bytes).decode("ascii"),
            "new_manifest_b64": base64.b64encode(new_manifest_bytes).decode("ascii"),
            "new_metadata_b64": base64.b64encode(
                migration_mod._json_bytes(new_metadata)
            ).decode("ascii"),
        },
    )

    with pytest.raises(migration_mod.MigrationValidationError, match="摘要"):
        migration_mod._recover_manifest_upgrade(first.run_dir)
    assert manifest_path.read_bytes() == old_manifest_bytes
    assert metadata_path.read_bytes() == old_metadata_bytes
    assert (first.run_dir / migration_mod.UPGRADE_JOURNAL_FILENAME).exists()


def test_manifest_staged_state_requires_path_and_hash(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest_path = first.run_dir / migration_mod.MANIFEST_FILENAME
    manifest = migration_mod.MigrationManifest.load(manifest_path)
    manifest.update(symbol, stage_path="", stage_sha256="")

    with pytest.raises(migration_mod.MigrationValidationError, match="evidence"):
        migration_mod.MigrationManifest.load(manifest_path)


def test_resume_rejects_reversed_metadata_window(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    metadata_path = first.run_dir / migration_mod.METADATA_FILENAME
    metadata = json.loads(metadata_path.read_text())
    metadata["start_date"] = "20260808"
    metadata["end_date"] = "20260807"
    migration_mod._write_json(metadata_path, metadata)

    with pytest.raises(migration_mod.MigrationValidationError, match="不能晚于"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_partial_same_source_run_blocks_new_run(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    warehouse_dir = tmp_path / "warehouse"
    root = migration_mod.get_migration_root(warehouse_dir)
    run_dir = root / "partial-run"
    run_dir.mkdir(parents=True)
    migration_mod._write_json(
        run_dir / migration_mod.METADATA_FILENAME,
        {
            "run_id": "partial-run",
            "manifest_schema_version": migration_mod.MANIFEST_SCHEMA_VERSION,
            "source": "sina-klc",
            "start_date": "19900101",
            "end_date": "20260807",
            "dry_run": False,
            "created_at": "2026-08-13T00:00:00",
        },
    )

    with pytest.raises(migration_mod.MigrationLockError, match="残留不完整"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            symbol="600009",
            end_date="20260807",
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


@pytest.mark.parametrize("status", ["pending", "fetching"])
def test_pre_cutoff_active_run_does_not_block_new_run(
    isolated_metadata, tmp_path, status
):
    insert_stock(isolated_metadata, "600009")
    insert_stock(isolated_metadata, "600010")
    warehouse_dir = tmp_path / "warehouse"
    root = migration_mod.get_migration_root(warehouse_dir)
    _, old_run_dir, old_manifest = migration_mod._build_run(
        root,
        f"old-{status}",
        [migration_mod.MigrationTarget("600009", "name-600009")],
        "sina-klc",
        "19900101",
        "20260807",
        False,
    )
    old_manifest.update("600009", status=status)

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600010",
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )

    assert result.staged_symbols == ("600010",)
    assert old_run_dir.exists()


@pytest.mark.parametrize("residual_kind", ["manifest", "journal", "broken_manifest"])
def test_partial_residual_without_metadata_blocks_new_run(
    isolated_metadata, tmp_path, residual_kind
):
    insert_stock(isolated_metadata, "600009")
    warehouse_dir = tmp_path / "warehouse"
    root = migration_mod.get_migration_root(warehouse_dir)
    run_dir = root / f"partial-{residual_kind}"
    run_dir.mkdir(parents=True)
    if residual_kind == "manifest":
        (run_dir / migration_mod.MANIFEST_FILENAME).write_text("broken")
    elif residual_kind == "journal":
        (run_dir / migration_mod.UPGRADE_JOURNAL_FILENAME).write_text("broken")
    else:
        (run_dir / migration_mod.MANIFEST_FILENAME).symlink_to(
            run_dir / "missing-manifest.csv"
        )

    with pytest.raises(migration_mod.MigrationLockError, match="metadata"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            symbol="600009",
            end_date="20260807",
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_unrelated_source_run_is_ignored(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    warehouse_dir = tmp_path / "warehouse"
    root = migration_mod.get_migration_root(warehouse_dir)
    run_dir = root / "other-source"
    run_dir.mkdir(parents=True)
    migration_mod._write_json(
        run_dir / migration_mod.METADATA_FILENAME,
        {
            "run_id": "other-source",
            "manifest_schema_version": 999,
            "source": "other-source",
            "start_date": "19900101",
            "end_date": "",
            "dry_run": True,
            "created_at": "2026-08-13T00:00:00",
        },
    )

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600009",
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    assert result.staged_symbols == ("600009",)


@pytest.mark.parametrize(
    "symlink_name",
    [
        migration_mod.METADATA_FILENAME,
        migration_mod.MANIFEST_FILENAME,
        migration_mod.UPGRADE_JOURNAL_FILENAME,
    ],
)
def test_run_file_symlink_blocks_new_run(isolated_metadata, tmp_path, symlink_name):
    insert_stock(isolated_metadata, "600009")
    warehouse_dir = tmp_path / "warehouse"
    root = migration_mod.get_migration_root(warehouse_dir)
    run_dir = root / f"symlink-{symlink_name}"
    run_dir.mkdir(parents=True)
    if symlink_name != migration_mod.METADATA_FILENAME:
        migration_mod._write_json(
            run_dir / migration_mod.METADATA_FILENAME,
            {
                "run_id": run_dir.name,
                "manifest_schema_version": migration_mod.MANIFEST_SCHEMA_VERSION,
                "source": "sina-klc",
                "start_date": "19900101",
                "end_date": "20260807",
                "dry_run": False,
                "created_at": "2026-08-13T00:00:00",
            },
        )
    (run_dir / symlink_name).symlink_to(run_dir / "missing-target")

    with pytest.raises(migration_mod.MigrationLockError, match="符号链接"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            symbol="600009",
            end_date="20260807",
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_legacy_v2_dry_run_requires_new_run(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    warehouse_dir = tmp_path / "warehouse"
    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600009",
        dry_run=True,
        warehouse_dir=warehouse_dir,
    )
    metadata_path = result.run_dir / migration_mod.METADATA_FILENAME
    manifest_path = result.run_dir / migration_mod.MANIFEST_FILENAME
    downgrade_metadata(metadata_path, migration_mod.LEGACY_MANIFEST_SCHEMA_VERSION)
    downgrade_manifest(
        manifest_path,
        {
            "cleanup_failed",
            "source_used",
            "weekend_rows_filtered",
            "known_bad_rows_filtered",
        },
    )
    metadata = json.loads(metadata_path.read_text())
    legacy_manifest = migration_mod.MigrationManifest.load(manifest_path)
    with pytest.raises(migration_mod.MigrationValidationError, match="不可变"):
        migration_mod._upgrade_legacy_run(result.run_dir, metadata, legacy_manifest)


def test_dry_run_allows_empty_end_date_metadata(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600009",
        end_date=None,
        dry_run=True,
        warehouse_dir=tmp_path / "warehouse",
    )
    metadata = json.loads(
        (result.run_dir / migration_mod.METADATA_FILENAME).read_text()
    )
    assert metadata["dry_run"] is True
    assert metadata["end_date"] == ""


def test_manifest_rejects_invalid_cleanup_failed_value(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest = migration_mod.MigrationManifest.load(
        first.run_dir / migration_mod.MANIFEST_FILENAME
    )
    manifest.update(symbol, cleanup_failed="true")

    with pytest.raises(migration_mod.MigrationValidationError, match="值无效"):
        migration_mod.MigrationManifest.load(
            first.run_dir / migration_mod.MANIFEST_FILENAME
        )


def test_manifest_rejects_staged_cleanup_failed_state(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest = migration_mod.MigrationManifest.load(
        first.run_dir / migration_mod.MANIFEST_FILENAME
    )
    manifest.update(symbol, cleanup_failed="1")

    with pytest.raises(migration_mod.MigrationValidationError, match="不得标记"):
        migration_mod.MigrationManifest.load(
            first.run_dir / migration_mod.MANIFEST_FILENAME
        )


def test_date_outside_requested_range_marks_failed(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)

    def fetcher(_sina_symbol, _start_date, _end_date):
        return make_quality_frame(dates=("2026-08-02", "2026-08-03"))

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        start_date="20260803",
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=fetcher,
    )

    assert result.failed_symbols == (symbol,)
    assert next(migration_mod.iter_manifest_rows(result.run_dir))["error_type"] == (
        "MigrationValidationError"
    )


def test_resume_rebuilds_tampered_staging_file(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    calls = []

    def fetcher(_sina_symbol, _start_date, _end_date):
        calls.append(True)
        return make_quality_frame()

    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=fetcher,
    )
    row = next(migration_mod.iter_manifest_rows(first.run_dir))
    Path(row["stage_path"]).write_bytes(b"tampered")

    resumed = migration_mod.run_kline_source_migration(
        source="sina-klc",
        resume_run_id=first.run_id,
        warehouse_dir=warehouse_dir,
        fetcher=fetcher,
    )

    assert resumed.failed_symbols == ()
    assert resumed.staged_symbols == (symbol,)
    assert len(calls) == 2
    assert Path(row["stage_path"]).stat().st_size > len(b"tampered")


def test_manifest_stage_path_outside_run_is_rejected(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"

    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest = migration_mod.MigrationManifest.load(
        first.run_dir / migration_mod.MANIFEST_FILENAME
    )
    manifest.update(symbol, stage_path=tmp_path / "outside.parquet")

    with pytest.raises(migration_mod.MigrationValidationError, match="越出"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_manifest_stage_path_for_other_symbol_is_rejected(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest = migration_mod.MigrationManifest.load(
        first.run_dir / migration_mod.MANIFEST_FILENAME
    )
    manifest.update(
        symbol,
        stage_path=first.run_dir / "staged" / "symbol=600010" / "data.parquet",
    )

    with pytest.raises(migration_mod.MigrationValidationError, match="不匹配"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_manifest_nested_stage_path_for_same_symbol_is_rejected(
    isolated_metadata, tmp_path
):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest = migration_mod.MigrationManifest.load(
        first.run_dir / migration_mod.MANIFEST_FILENAME
    )
    manifest.update(
        symbol,
        stage_path=first.run_dir
        / "staged"
        / "nested"
        / f"symbol={symbol}"
        / "data.parquet",
    )

    with pytest.raises(migration_mod.MigrationValidationError, match="标准路径"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_staged_manifest_without_hash_is_rejected(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    first = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=lambda *_args: make_quality_frame(),
    )
    manifest = migration_mod.MigrationManifest.load(
        first.run_dir / migration_mod.MANIFEST_FILENAME
    )
    manifest.update(symbol, stage_sha256="")

    with pytest.raises(migration_mod.MigrationValidationError, match="stage_sha256"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            resume_run_id=first.run_id,
            warehouse_dir=warehouse_dir,
            fetcher=lambda *_args: make_quality_frame(),
        )


def test_staging_parent_symlink_is_rejected(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (run_dir / "staged").symlink_to(outside, target_is_directory=True)

    with pytest.raises(migration_mod.MigrationValidationError, match="根目录"):
        migration_mod._safe_stage_path(
            run_dir, str(run_dir / "staged" / "symbol=600009" / "data.parquet")
        )


def test_old_partition_is_ignored_during_staging(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    old_frame = make_frame(
        dates=("2026-08-02", "2026-08-03"),
        factor=(1.0, 1.0),
        close=(9.0, 10.0),
        close_hfq=(9.0, 10.0),
    )
    write_canonical(warehouse_dir, symbol, old_frame)

    def fetcher(_sina_symbol, _start_date, _end_date):
        return make_quality_frame(
            dates=("2026-08-03", "2026-08-04"),
            factor=(2.0, 2.0),
            close=(10.0, 11.0),
            close_hfq=(20.0, 22.0),
        )

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=warehouse_dir,
        fetcher=fetcher,
    )

    assert result.failed_symbols == ()
    assert result.staged_symbols == (symbol,)


def test_stale_lock_requires_explicit_recovery(tmp_path):
    root = tmp_path / "migration-root"
    root.mkdir()
    lock_path = root / migration_mod.LOCK_FILENAME
    lock_path.write_text("run_id=old\npid=999999999\n", encoding="utf-8")

    with migration_mod.MigrationLock(root, "new", recover_stale=True):
        assert lock_path.exists()
    assert lock_path.read_bytes() == b""


def test_active_lock_cannot_be_recovered(tmp_path):
    root = tmp_path / "migration-root"
    root.mkdir()
    lock_path = root / migration_mod.LOCK_FILENAME
    lock_path.write_text(f"run_id=old\npid={os.getpid()}\n", encoding="utf-8")

    with pytest.raises(migration_mod.MigrationLockError, match="已有迁移"):
        with migration_mod.MigrationLock(root, "new", recover_stale=True):
            pass


def test_invalid_lock_pid_cannot_be_recovered(tmp_path):
    root = tmp_path / "migration-root"
    root.mkdir()
    lock_path = root / migration_mod.LOCK_FILENAME
    lock_path.write_text("run_id=old\n", encoding="utf-8")

    with pytest.raises(migration_mod.MigrationLockError, match="缺少有效 PID"):
        with migration_mod.MigrationLock(root, "new", recover_stale=True):
            pass


def test_lock_symlink_is_rejected_without_following_target(tmp_path):
    root = tmp_path / "migration-root"
    root.mkdir()
    outside = tmp_path / "outside-lock"
    outside.write_text("do not modify", encoding="utf-8")
    (root / migration_mod.LOCK_FILENAME).symlink_to(outside)

    with pytest.raises(migration_mod.MigrationLockError, match="符号链接"):
        with migration_mod.MigrationLock(root, "new"):
            pass

    assert outside.read_text(encoding="utf-8") == "do not modify"


def test_lock_failure_does_not_leave_pending_run(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    warehouse_dir = tmp_path / "warehouse"
    root = migration_mod.get_migration_root(warehouse_dir)
    root.mkdir(parents=True)

    with migration_mod.MigrationLock(root, "existing"):
        with pytest.raises(migration_mod.MigrationLockError):
            migration_mod.run_kline_source_migration(
                source="sina-klc",
                symbol=symbol,
                end_date="20260807",
                warehouse_dir=warehouse_dir,
                fetcher=lambda *_args: make_quality_frame(),
            )

    assert not list(root.glob("*/"))


def test_lock_and_manifest_use_same_run_id(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600009",
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: make_quality_frame(),
    )
    with (result.run_dir / migration_mod.METADATA_FILENAME).open() as file:
        metadata = json.load(file)
    assert metadata["run_id"] == result.run_id == result.run_dir.name


@pytest.mark.parametrize("write_mode", ["error", "short"])
def test_lock_write_failure_cleans_owned_lock(tmp_path, monkeypatch, write_mode):
    root = tmp_path / "migration-root"

    def broken_write(_fd, _payload):
        if write_mode == "error":
            raise OSError("write failed")
        return 0

    monkeypatch.setattr(migration_mod.os, "write", broken_write)
    with pytest.raises(OSError, match="write failed|长度非法"):
        with migration_mod.MigrationLock(root, "run"):
            pass
    lock_path = root / migration_mod.LOCK_FILENAME
    assert lock_path.exists()
    assert lock_path.read_bytes() == b""


def test_lock_release_closes_fd_when_truncate_fails(tmp_path, monkeypatch):
    root = tmp_path / "migration-root"
    original_ftruncate = migration_mod.os.ftruncate
    original_close = migration_mod.os.close
    closed = []

    lock = migration_mod.MigrationLock(root, "run")
    lock.__enter__()

    def fail_on_release(fd, length):
        if length == 0:
            raise OSError("truncate failed")
        return original_ftruncate(fd, length)

    monkeypatch.setattr(migration_mod.os, "ftruncate", fail_on_release)
    monkeypatch.setattr(
        migration_mod.os,
        "close",
        lambda fd: closed.append(fd) or original_close(fd),
    )
    with pytest.raises(OSError, match="truncate failed"):
        lock.__exit__(None, None, None)

    assert closed
    assert lock._owned is False


def test_zero_inter_symbol_delay_is_rejected(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "600009")
    with pytest.raises(ValueError, match="正数"):
        migration_mod.run_kline_source_migration(
            source="sina-klc",
            symbol="600009",
            end_date="20260807",
            inter_symbol_delay=(0.0, 0.0),
            dry_run=True,
            warehouse_dir=tmp_path / "warehouse",
        )


def test_cli_exposes_staging_options():
    project_root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, "main.py", "migrate-kline-source", "--help"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    for option in ("--source", "--stage-only", "--resume", "--recover-stale-lock"):
        assert option in completed.stdout


def test_cli_requires_stage_only():
    project_root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            "main.py",
            "migrate-kline-source",
            "--source",
            "sina-klc",
            "--dry-run",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "--stage-only" in completed.stderr


def test_sina_block_stops_remaining_symbols(isolated_metadata, tmp_path):
    for symbol in ("600009", "600010"):
        insert_stock(isolated_metadata, symbol)
    calls = []

    def fetcher(sina_symbol, _start_date, _end_date):
        calls.append(sina_symbol)
        raise SinaBlockedError("blocked")

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        limit=2,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=fetcher,
    )

    assert result.stopped_by_sina_block is True
    assert result.failed_symbols == ("600009",)
    assert calls == ["sh600009"]


def test_delisted_default_fetcher_falls_back_to_tencent(
    isolated_metadata, tmp_path, monkeypatch
):
    insert_stock(isolated_metadata, "000003", active=0)
    frame = make_quality_frame()
    frame.attrs["source"] = "tencent-newfq"
    monkeypatch.setattr(
        migration_mod.SinaKlcFetcher,
        "fetch_klc_data",
        staticmethod(lambda *_args, **_kwargs: pd.DataFrame()),
    )
    monkeypatch.setattr(
        migration_mod.TencentKlineFetcher,
        "fetch_full",
        staticmethod(lambda *args, **kwargs: frame),
    )

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="000003",
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
    )

    assert result.failed_symbols == ()
    assert result.staged_symbols == ("000003",)
    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert row["source_used"] == "tencent-newfq"


def test_delisted_tencent_weekend_data_is_not_staged(
    isolated_metadata, tmp_path, monkeypatch
):
    insert_stock(isolated_metadata, "000003", active=0)
    frame = make_quality_frame(dates=("2026-08-08", "2026-08-10"))
    frame.attrs["source"] = "tencent-newfq"
    monkeypatch.setattr(
        migration_mod.SinaKlcFetcher,
        "fetch_klc_data",
        staticmethod(lambda *_args, **_kwargs: pd.DataFrame()),
    )
    monkeypatch.setattr(
        migration_mod.TencentKlineFetcher,
        "fetch_full",
        staticmethod(lambda *args, **kwargs: frame),
    )

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="000003",
        end_date="20260810",
        warehouse_dir=tmp_path / "warehouse",
    )

    assert result.staged_symbols == ()
    assert result.failed_symbols == ("000003",)
    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert row["status"] == "failed"
    assert row["error_type"] == "MigrationQualityError"
    assert row["stage_path"] == ""
    assert not list((result.run_dir / "staged").rglob("*.parquet"))


def test_tencent_fetch_failure_preserves_source_provenance(isolated_metadata, tmp_path):
    insert_stock(isolated_metadata, "000003", active=0)

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="000003",
        end_date="20260810",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: (_ for _ in ()).throw(
            TencentKlineFetchError("腾讯质量校验失败")
        ),
    )

    assert result.failed_symbols == ("000003",)
    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert row["source_used"] == "tencent-newfq"
    assert row["error_type"] == "TencentKlineFetchError"


def test_staging_rejects_unknown_fetcher_source(isolated_metadata, tmp_path):
    symbol = "600009"
    insert_stock(isolated_metadata, symbol)
    frame = make_quality_frame()
    frame.attrs["source"] = "unknown-source"

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol=symbol,
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
        fetcher=lambda *_args: frame,
    )

    assert result.failed_symbols == (symbol,)
    row = next(migration_mod.iter_manifest_rows(result.run_dir))
    assert row["source_used"] == ""
    assert row["error_type"] == "MigrationValidationError"


def test_active_default_fetcher_does_not_fall_back_to_tencent(
    isolated_metadata, tmp_path, monkeypatch
):
    insert_stock(isolated_metadata, "600009", active=1)
    tencent_called = []
    monkeypatch.setattr(
        migration_mod.SinaKlcFetcher,
        "fetch_klc_data",
        staticmethod(
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("KLC failed"))
        ),
    )
    monkeypatch.setattr(
        migration_mod.TencentKlineFetcher,
        "fetch_full",
        staticmethod(lambda *args, **kwargs: tencent_called.append(1)),
    )

    result = migration_mod.run_kline_source_migration(
        source="sina-klc",
        symbol="600009",
        end_date="20260807",
        warehouse_dir=tmp_path / "warehouse",
    )

    assert result.failed_symbols == ("600009",)
    assert tencent_called == []
