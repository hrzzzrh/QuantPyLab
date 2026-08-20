from pathlib import Path

import pandas as pd
import pytest

import storage.file_store.atomic_snapshot_store as snapshot_mod
from storage.file_store.atomic_snapshot_store import save_snapshot_atomically
from utils.canonical_write_lock import CanonicalWriteLock, CanonicalWriteLockError

CATEGORY = "industry_classification_sw"


def _frame(value: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "effective_date": [pd.Timestamp("2020-01-01").date()],
            "industry_code": [value],
            "source_updated_date": [pd.Timestamp("2026-08-21").date()],
        }
    )


def _save(base_dir: Path, partitions: list[tuple[pd.DataFrame, str]]) -> None:
    save_snapshot_atomically(
        base_dir,
        CATEGORY,
        partitions,
        operation="test-industry-snapshot",
        run_id="test-run",
    )


def _canonical(base_dir: Path, symbol: str) -> Path:
    return base_dir / CATEGORY / f"symbol={symbol}" / "data.parquet"


def test_snapshot_promotion_replaces_full_dataset_and_removes_stale_symbols(
    tmp_path,
):
    _save(tmp_path, [(_frame("480101"), "000001"), (_frame("480102"), "000002")])

    _save(tmp_path, [(_frame("480301"), "000001")])

    assert pd.read_parquet(_canonical(tmp_path, "000001"))[
        "industry_code"
    ].tolist() == ["480301"]
    assert not _canonical(tmp_path, "000002").exists()
    assert list((tmp_path / CATEGORY).glob(".*backup-*")) == []
    assert list((tmp_path / ".staging" / CATEGORY).glob("run-*")) == []


def test_staging_failure_keeps_previous_snapshot_and_cleans_run(tmp_path, monkeypatch):
    _save(tmp_path, [(_frame("480101"), "000001")])

    def fail_staging(*_args, **_kwargs):
        raise OSError("模拟 staging 写入失败")

    monkeypatch.setattr(snapshot_mod, "save_partitions_atomically", fail_staging)

    with pytest.raises(OSError, match="模拟 staging 写入失败"):
        _save(tmp_path, [(_frame("480301"), "000001")])

    assert pd.read_parquet(_canonical(tmp_path, "000001"))[
        "industry_code"
    ].tolist() == ["480101"]
    assert list((tmp_path / ".staging" / CATEGORY).glob("run-*")) == []


def test_promotion_failure_rolls_back_previous_snapshot(tmp_path, monkeypatch):
    _save(tmp_path, [(_frame("480101"), "000001")])
    original_replace = snapshot_mod.os.replace

    def fail_staged_promotion(source, target):
        if ".staging" in Path(source).parts:
            raise OSError("模拟快照晋级失败")
        return original_replace(source, target)

    monkeypatch.setattr(snapshot_mod.os, "replace", fail_staged_promotion)

    with pytest.raises(OSError, match="模拟快照晋级失败"):
        _save(tmp_path, [(_frame("480301"), "000001")])

    assert pd.read_parquet(_canonical(tmp_path, "000001"))[
        "industry_code"
    ].tolist() == ["480101"]
    assert list((tmp_path / CATEGORY).glob(".*backup-*")) == []
    assert list((tmp_path / ".staging" / CATEGORY).glob("run-*")) == []


def test_snapshot_promotion_rejects_concurrent_writer_and_preserves_snapshot(tmp_path):
    _save(tmp_path, [(_frame("480101"), "000001")])

    with CanonicalWriteLock(tmp_path, operation="test-holder"):
        with pytest.raises(CanonicalWriteLockError, match="已被占用"):
            _save(tmp_path, [(_frame("480301"), "000001")])

    assert pd.read_parquet(_canonical(tmp_path, "000001"))[
        "industry_code"
    ].tolist() == ["480101"]
    assert list((tmp_path / ".staging" / CATEGORY).glob("run-*")) == []
