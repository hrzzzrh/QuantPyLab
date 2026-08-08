"""单元测试: storage/database/view_loader.py 视图发现、拓扑排序与 PlantUML 生成"""

from pathlib import Path

import pytest

from storage.database.view_loader import ViewLoader

VIEW_TEMPLATE = """\
from storage.database.view_base import DuckDBView

class {cls}(DuckDBView):
    name = {name!r}
    dependencies = {deps!r}

    def get_sql(self, warehouse_dir: str) -> str:
        return f"CREATE OR REPLACE VIEW {name} AS SELECT 1"
"""


def _write_view_file(views_dir: Path, filename: str, content: str):
    path = views_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _view_file(cls: str, name: str, deps=None) -> str:
    return VIEW_TEMPLATE.format(cls=cls, name=name, deps=deps or [])


@pytest.fixture
def loader_factory(tmp_path):
    def make(files: dict) -> ViewLoader:
        views_dir = tmp_path / "views"
        for filename, content in files.items():
            _write_view_file(views_dir, filename, content)
        loader = ViewLoader(views_dir)
        loader.discover_views()
        return loader

    return make


class TestDiscoverViews:
    def test_discovers_all_view_classes(self, loader_factory):
        loader = loader_factory(
            {
                "a.py": _view_file("ViewA", "view_a"),
                "b.py": _view_file("ViewB", "view_b"),
            }
        )
        assert set(loader.view_classes) == {"view_a", "view_b"}

    def test_skips_non_view_classes(self, loader_factory):
        loader = loader_factory(
            {
                "a.py": (
                    "from storage.database.view_base import DuckDBView\n"
                    "class ViewA(DuckDBView):\n"
                    "    name = 'view_a'\n"
                    "    def get_sql(self, warehouse_dir: str) -> str:\n"
                    "        return ''\n"
                    "def helper(): return 1\n"
                    "class NotAView: pass\n"
                )
            }
        )
        assert set(loader.view_classes) == {"view_a"}

    def test_empty_dir(self, tmp_path):
        loader = ViewLoader(tmp_path / "nonexistent")
        loader.discover_views()
        assert loader.view_classes == {}


class TestTopologicalSort:
    def test_dependency_comes_first(self, loader_factory):
        loader = loader_factory(
            {
                "a.py": _view_file("ViewA", "view_a", deps=["view_b"]),
                "b.py": _view_file("ViewB", "view_b"),
            }
        )
        order = [v.name for v in loader.get_sorted_views()]
        assert order.index("view_b") < order.index("view_a")

    def test_chain_dependencies(self, loader_factory):
        loader = loader_factory(
            {
                "a.py": _view_file("ViewA", "view_a", deps=["view_b"]),
                "b.py": _view_file("ViewB", "view_b", deps=["view_c"]),
                "c.py": _view_file("ViewC", "view_c"),
            }
        )
        order = [v.name for v in loader.get_sorted_views()]
        assert order == ["view_c", "view_b", "view_a"]

    def test_cyclic_dependency_raises(self, loader_factory):
        loader = loader_factory(
            {
                "a.py": _view_file("ViewA", "view_a", deps=["view_b"]),
                "b.py": _view_file("ViewB", "view_b", deps=["view_a"]),
            }
        )
        with pytest.raises(ValueError, match="循环引用|CycleError"):
            loader.get_sorted_views()

    def test_missing_dependency_is_tolerated(self, loader_factory):
        """依赖未在本目录注册时不应导致崩溃 (外部依赖被忽略)"""
        loader = loader_factory(
            {
                "a.py": _view_file("ViewA", "view_a", deps=["view_b"]),
            }
        )
        order = [v.name for v in loader.get_sorted_views()]
        assert order == ["view_a"]


class TestGeneratePuml:
    def test_contains_components_and_edges(self, loader_factory):
        loader = loader_factory(
            {
                "a.py": _view_file("ViewA", "view_a", deps=["view_b"]),
                "b.py": _view_file("ViewB", "view_b"),
            }
        )
        puml = loader.generate_puml()
        assert puml.startswith("@startuml")
        assert puml.endswith("@enduml")
        assert "[view_a]" in puml
        assert "[view_b]" in puml
        assert "[view_b] --> [view_a]" in puml
