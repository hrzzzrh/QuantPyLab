"""测试多 Parquet 分区的事务式替换。"""

from pathlib import Path

import pandas as pd
import pytest

import storage.file_store.atomic_partition_store as atomic_store_mod
from storage.file_store.atomic_partition_store import save_partitions_atomically


def test_save_partitions_atomically_rolls_back_on_failure(tmp_path, monkeypatch):
    first_category = "financial_statements/type=income"
    second_category = "financial_statements/type=cashflow"

    save_partitions_atomically(
        tmp_path,
        [
            (pd.DataFrame({"value": [1]}), first_category, "000001"),
            (pd.DataFrame({"value": [2]}), second_category, "000001"),
        ],
    )

    original_replace = atomic_store_mod.os.replace
    replace_calls = 0

    def fail_on_second_new_file(src, dst):
        nonlocal replace_calls
        replace_calls += 1
        if replace_calls == 4:
            raise OSError("模拟第二个分区提交失败")
        return original_replace(src, dst)

    monkeypatch.setattr(atomic_store_mod.os, "replace", fail_on_second_new_file)

    with pytest.raises(OSError, match="模拟第二个分区提交失败"):
        save_partitions_atomically(
            tmp_path,
            [
                (pd.DataFrame({"value": [10]}), first_category, "000001"),
                (pd.DataFrame({"value": [20]}), second_category, "000001"),
            ],
        )

    first_path = tmp_path / first_category / "symbol=000001/data.parquet"
    second_path = tmp_path / second_category / "symbol=000001/data.parquet"
    assert pd.read_parquet(first_path)["value"].tolist() == [1]
    assert pd.read_parquet(second_path)["value"].tolist() == [2]
    assert not list(Path(tmp_path).rglob(".tmp_*.parquet"))
    assert not list(Path(tmp_path).rglob(".backup_*.parquet"))


def test_backup_cleanup_failure_keeps_committed_partition_and_backup(
    tmp_path, monkeypatch
):
    category = "financial_statements/type=income"
    save_partitions_atomically(
        tmp_path,
        [(pd.DataFrame({"value": [1]}), category, "000001")],
    )

    original_unlink = Path.unlink

    def fail_backup_cleanup(path, missing_ok=False):
        if path.name.startswith(".backup_"):
            raise OSError("模拟备份清理失败")
        return original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_backup_cleanup)

    save_partitions_atomically(
        tmp_path,
        [(pd.DataFrame({"value": [2]}), category, "000001")],
    )

    target_path = tmp_path / category / "symbol=000001/data.parquet"
    assert pd.read_parquet(target_path)["value"].tolist() == [2]
    backups = list(Path(tmp_path).rglob(".backup_*.parquet"))
    assert len(backups) == 1
    assert pd.read_parquet(backups[0])["value"].tolist() == [1]
    assert not list(Path(tmp_path).rglob(".tmp_*.parquet"))


def test_temp_partition_is_removed_when_write_fails(tmp_path, monkeypatch):
    def fail_write(df, temp_path):
        temp_path.touch()
        raise OSError("模拟临时文件写入失败")

    monkeypatch.setattr(atomic_store_mod, "_write_temp_partition", fail_write)

    with pytest.raises(OSError, match="模拟临时文件写入失败"):
        save_partitions_atomically(
            tmp_path,
            [(pd.DataFrame({"value": [1]}), "financial/ttm", "000001")],
        )

    assert not list(Path(tmp_path).rglob(".tmp_*.parquet"))
