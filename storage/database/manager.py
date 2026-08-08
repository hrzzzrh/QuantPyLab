import sqlite3
from datetime import datetime
from pathlib import Path

import duckdb

from config.settings import SQLITE_DB_PATH, WAREHOUSE_DIR

from .view_loader import ViewLoader


class DBManager:
    """
    数据库管理器，负责管理 SQLite (元数据) 和 DuckDB (分析数据) 的连接。
    遵循读写分离原则，统一通过此类获取连接。
    """

    def __init__(self):
        self.sqlite_path = SQLITE_DB_PATH
        self.warehouse_dir = Path(WAREHOUSE_DIR)

        self._sqlite_conn: sqlite3.Connection | None = None
        self._duckdb_conn: duckdb.DuckDBPyConnection | None = None

        # 初始化表结构
        self.initialize_schema()

    def initialize_schema(self):
        """初始化 SQLite 元数据表结构"""
        conn = self.get_sqlite_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                area TEXT,
                industry TEXT,
                list_date TEXT,
                is_active INTEGER DEFAULT 1,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etfs (
                symbol TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                fund_type TEXT,
                list_date TEXT,
                is_active INTEGER DEFAULT 1,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

    def get_sqlite_conn(self) -> sqlite3.Connection:
        """获取 SQLite 连接 (元数据)"""
        if self._sqlite_conn is None:
            self._sqlite_conn = sqlite3.connect(
                self.sqlite_path, check_same_thread=False
            )
            # 启用外键约束
            self._sqlite_conn.execute("PRAGMA foreign_keys = ON;")
        return self._sqlite_conn

    def get_duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        """获取 DuckDB 连接 (作为瞬态计算引擎)。

        视图采用按需注册 (Lazy View Loading)：
        默认不注册任何视图，调用方通过 ensure_views() 声明所需视图，
        避免一次性加载全部视图导致的分片元数据扫描内存暴涨。
        """
        if self._duckdb_conn is None:
            # 使用内存模式
            self._duckdb_conn = duckdb.connect(":memory:")
        return self._duckdb_conn

    def _get_view_loader(self) -> ViewLoader:
        views_dir = Path(__file__).parent / "views"
        loader = ViewLoader(views_dir)
        loader.discover_views()
        return loader

    def ensure_views(self, *view_names: str):
        """按需注册指定视图（含其依赖），已注册的视图自动跳过。

        使用 DAG 拓扑排序保证依赖视图先于依赖者创建。
        """
        from utils.logger import logger

        conn = self.get_duckdb_conn()
        loader = self._get_view_loader()

        registered = set(self.list_available_views())
        needed = set(view_names)
        instances = {n: cls() for n, cls in loader.view_classes.items()}
        # 收集全部传递依赖
        changed = True
        while changed:
            changed = False
            for name in list(needed):
                inst = instances.get(name)
                if inst is None:
                    continue
                for dep in inst.dependencies:
                    if dep not in needed:
                        needed.add(dep)
                        changed = True

        to_create = [
            n for n in needed if n in loader.view_classes and n not in registered
        ]
        if not to_create:
            return

        # 拓扑排序确保依赖先创建
        from graphlib import TopologicalSorter

        ts = TopologicalSorter()
        for name in to_create:
            inst = instances[name]
            deps = [d for d in inst.dependencies if d in to_create]
            ts.add(name, *deps)
        try:
            order = list(ts.static_order())
        except Exception as e:
            raise ValueError(f"视图依赖图循环引用: {e}")

        created = []
        for name in order:
            if name not in to_create:
                continue
            try:
                cls = loader.view_classes[name]
                conn.execute(cls().get_sql(str(self.warehouse_dir)))
                created.append(name)
            except Exception:
                logger.exception(f"按需加载视图失败 {name}")
        if created:
            logger.info(f"按需加载视图: {', '.join(created)}")

    def init_warehouse_views(self, conn: duckdb.DuckDBPyConnection):
        """扫描并注册全部视图（全量模式，仅在明确需要时调用）"""
        from utils.logger import logger

        loader = self._get_view_loader()
        sorted_views = loader.get_sorted_views()
        for view in sorted_views:
            try:
                sql = view.get_sql(str(self.warehouse_dir))
                conn.execute(sql)
            except Exception:
                logger.exception(f"加载视图失败 {view.name}")
        logger.info(f"成功加载 {len(sorted_views)} 个视图")

    def get_view_relationships_puml(self) -> str:
        """获取当前视图依赖关系的 PlantUML 源码"""
        views_dir = Path(__file__).parent / "views"
        loader = ViewLoader(views_dir)
        loader.discover_views()
        return loader.generate_puml()

    def generate_full_sql(self) -> str:
        """生成包含所有视图定义的完整 SQL 脚本"""
        views_dir = Path(__file__).parent / "views"
        loader = ViewLoader(views_dir)
        loader.discover_views()
        sorted_views = loader.get_sorted_views()

        sql_blocks = [
            "-- QuantPyLab 自动生成的视图脚本",
            "-- 运行环境: DuckDB",
            f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "\n",
        ]

        for view in sorted_views:
            sql = view.get_sql(str(self.warehouse_dir)).strip()
            if not sql.endswith(";"):
                sql += ";"
            sql_blocks.append(f"-- View: {view.name}")
            sql_blocks.append(sql)
            sql_blocks.append("")

        return "\n".join(sql_blocks)

    def list_available_views(self) -> list[str]:
        """获取当前 DuckDB 中可用的所有视图列表"""
        conn = self.get_duckdb_conn()
        res = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'"
        ).fetchall()
        return [row[0] for row in res]

    def close_all(self):
        """关闭所有数据库连接"""
        if self._sqlite_conn:
            self._sqlite_conn.close()
            self._sqlite_conn = None
        if self._duckdb_conn:
            self._duckdb_conn.close()
            self._duckdb_conn = None


# 创建全局单例
db_manager = DBManager()
