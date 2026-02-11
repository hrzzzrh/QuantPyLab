from abc import ABC, abstractmethod
from typing import List

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
