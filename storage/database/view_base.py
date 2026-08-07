from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from .schema_builder import ensure_schema


def build_schema_map_expr(dataset: str) -> str:
    """
    根据 schema 缓存生成 read_parquet 的 schema MAP 表达式。
    缓存缺失时自动重建。
    """
    schema: Dict[str, str] = ensure_schema(dataset)
    parts = [
        f"'{name}': {{'name': '{name}', 'type': '{dtype}', 'default_value': NULL}}"
        for name, dtype in sorted(schema.items())
    ]
    return "{" + ", ".join(parts) + "}::MAP(VARCHAR, STRUCT(name VARCHAR, type VARCHAR, default_value VARCHAR))"


class DuckDBView(ABC):
    """
    DuckDB 视图定义基类。
    所有视图都应继承此类，并实现必要的属性和方法。
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """视图名称 (也是数据库中的表名)"""
        pass

    @property
    def dependencies(self) -> List[str]:
        """
        依赖的其他视图名称列表。
        用于构建 DAG 并确定加载顺序。
        """
        return []

    @abstractmethod
    def get_sql(self, warehouse_dir: str) -> str:
        """
        获取创建视图的完整 SQL 语句。
        :param warehouse_dir: 数据仓库的绝对路径，用于替换 SQL 中的占位符
        """
        pass
