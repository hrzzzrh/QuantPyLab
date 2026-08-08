"""单元测试: storage/file_store/parquet_store.py Parquet 原子写入与分片管理"""

import pandas as pd
import pytest

import storage.file_store.parquet_store as parquet_store_mod
from storage.file_store.parquet_store import ParquetStore


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(parquet_store_mod, "WAREHOUSE_DIR", tmp_path)
    return ParquetStore(), tmp_path


class TestSavePartition:
    def test_saves_hive_partitioned_file(self, store):
        s, base = store
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        s.save_partition(df, "financial_statements/type=income", "000001")

        path = base / "financial_statements/type=income/symbol=000001/data.parquet"
        assert path.exists()
        result = pd.read_parquet(path)
        assert result.equals(df)

    def test_excludes_symbol_column(self, store):
        """DataFrame 含 symbol 列时应排除 (已存在于目录名)"""
        s, base = store
        df = pd.DataFrame({"symbol": ["000001"] * 2, "a": [1, 2]})
        s.save_partition(df, "daily_kline", "000001")

        path = base / "daily_kline/symbol=000001/data.parquet"
        result = pd.read_parquet(path)
        assert "symbol" not in result.columns
        assert result["a"].tolist() == [1, 2]

    def test_overwrite_existing_partition(self, store):
        s, base = store
        s.save_partition(pd.DataFrame({"a": [1]}), "daily_kline", "000001")
        s.save_partition(pd.DataFrame({"a": [2, 3]}), "daily_kline", "000001")

        path = base / "daily_kline/symbol=000001/data.parquet"
        assert pd.read_parquet(path)["a"].tolist() == [2, 3]

    def test_empty_df_does_not_create_directory(self, store):
        s, base = store
        s.save_partition(pd.DataFrame(), "daily_kline", "000001")
        assert not (base / "daily_kline").exists()

    def test_no_temp_files_left_after_save(self, store):
        s, base = store
        s.save_partition(pd.DataFrame({"a": [1]}), "daily_kline", "000001")
        leftovers = list((base / "daily_kline/symbol=000001").glob(".tmp_*"))
        assert leftovers == []


class TestGetPath:
    def test_glob_pattern(self, store):
        s, base = store
        assert s.get_path("daily_kline") == str(base / "daily_kline/*/*.parquet")
