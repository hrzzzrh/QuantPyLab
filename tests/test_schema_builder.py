"""单元测试: storage/database/schema_builder.py schema 缓存与类型加宽"""

import json

import pytest

import storage.database.schema_builder as schema_builder_mod
from storage.database.schema_builder import (
    _wider_type,
    ensure_schema,
    load_schema,
    save_schema,
)


@pytest.fixture
def isolated_schema_dir(tmp_path, monkeypatch):
    """将 schema 缓存目录指向临时目录, 避免污染真实 views/schemas"""
    monkeypatch.setattr(schema_builder_mod, "SCHEMA_DIR", tmp_path / "schemas")
    return tmp_path / "schemas"


class TestWiderType:
    def test_string_promoted_to_int(self):
        assert _wider_type("VARCHAR", "BIGINT") == "BIGINT"

    def test_int_promoted_to_float(self):
        assert _wider_type("INTEGER", "FLOAT") == "FLOAT"

    def test_float_promoted_to_double(self):
        assert _wider_type("FLOAT", "DOUBLE") == "DOUBLE"

    def test_bigint_wider_than_integer(self):
        assert _wider_type("BIGINT", "INTEGER") == "BIGINT"

    def test_same_type_kept(self):
        assert _wider_type("DOUBLE", "DOUBLE") == "DOUBLE"

    def test_unknown_type_wins(self):
        """未知类型 (如 DECIMAL) 应被视为最宽, 避免精度损失"""
        assert _wider_type("DECIMAL", "BIGINT") == "DECIMAL"
        assert _wider_type("BIGINT", "DECIMAL") == "DECIMAL"


class TestSchemaCacheRoundtrip:
    def test_save_and_load(self, isolated_schema_dir):
        save_schema("fin_ttm", {"a": "BIGINT", "b": "VARCHAR"})
        assert load_schema("fin_ttm") == {"a": "BIGINT", "b": "VARCHAR"}

    def test_missing_cache_returns_none(self, isolated_schema_dir):
        assert load_schema("fin_ttm") is None

    def test_corrupted_cache_returns_none(self, isolated_schema_dir):
        path = isolated_schema_dir / "fin_ttm.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not valid json", encoding="utf-8")
        assert load_schema("fin_ttm") is None

    def test_empty_columns_returns_none(self, isolated_schema_dir):
        save_schema("fin_ttm", {})
        assert load_schema("fin_ttm") is None

    def test_non_dict_columns_returns_none(self, isolated_schema_dir):
        path = isolated_schema_dir / "fin_ttm.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"columns": []}), encoding="utf-8")
        assert load_schema("fin_ttm") is None


class TestEnsureSchema:
    def test_returns_cache_without_rebuild(self, isolated_schema_dir, monkeypatch):
        save_schema("fin_ttm", {"a": "BIGINT"})

        def fail_if_called(pattern):
            raise AssertionError("缓存存在时不应重建")

        monkeypatch.setattr(schema_builder_mod, "build_schema", fail_if_called)
        assert ensure_schema("fin_ttm") == {"a": "BIGINT"}

    def test_rebuilds_when_cache_missing(self, isolated_schema_dir, monkeypatch):
        monkeypatch.setattr(
            schema_builder_mod,
            "build_schema",
            lambda pattern: {"x": "DOUBLE", "y": "VARCHAR"},
        )
        assert ensure_schema("fin_ttm") == {"x": "DOUBLE", "y": "VARCHAR"}
        assert load_schema("fin_ttm") == {"x": "DOUBLE", "y": "VARCHAR"}

    def test_unknown_dataset_raises(self, isolated_schema_dir):
        with pytest.raises(ValueError, match="未知数据集"):
            ensure_schema("nonexistent_dataset")


class TestBuildSchemaMapExpr:
    def test_generates_map_expression(self, monkeypatch):
        import storage.database.view_base as view_base_mod

        monkeypatch.setattr(
            view_base_mod,
            "ensure_schema",
            lambda dataset: {"a": "BIGINT", "b": "VARCHAR"},
        )
        expr = view_base_mod.build_schema_map_expr("fin_ttm")
        assert "'a': {'name': 'a', 'type': 'BIGINT'" in expr
        assert "'b': {'name': 'b', 'type': 'VARCHAR'" in expr
        assert expr.endswith("::MAP(VARCHAR, STRUCT(name VARCHAR, type VARCHAR, default_value VARCHAR))")
