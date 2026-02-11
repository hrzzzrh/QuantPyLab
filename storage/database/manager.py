import sqlite3
import duckdb
import os
from pathlib import Path
from config.settings import SQLITE_DB_PATH, WAREHOUSE_DIR
from typing import Optional, List
from .view_loader import ViewLoader

class DBManager:
    """
    数据库管理器，负责管理 SQLite (元数据) 和 DuckDB (分析数据) 的连接。
    遵循读写分离原则，统一通过此类获取连接。
    """
    
    def __init__(self):
        self.sqlite_path = SQLITE_DB_PATH
        self.warehouse_dir = Path(WAREHOUSE_DIR)
        
        self._sqlite_conn: Optional[sqlite3.Connection] = None
        self._duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        
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
        conn.commit()

    def get_sqlite_conn(self) -> sqlite3.Connection:
        """获取 SQLite 连接 (元数据)"""
        if self._sqlite_conn is None:
            self._sqlite_conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
            # 启用外键约束
            self._sqlite_conn.execute("PRAGMA foreign_keys = ON;")
        return self._sqlite_conn

    def get_duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        """获取 DuckDB 连接 (作为瞬态计算引擎)"""
        if self._duckdb_conn is None:
            # 使用内存模式
            self._duckdb_conn = duckdb.connect(":memory:")
            # 自动挂载 Parquet 数据湖中的视图
            self.init_warehouse_views(self._duckdb_conn)
        return self._duckdb_conn

    def init_warehouse_views(self, conn: duckdb.DuckDBPyConnection):
        """扫描并加载所有视图，支持基于 DAG 的自动依赖处理"""
        from utils.logger import logger
        
        views_dir = Path(__file__).parent / "views"
        if not views_dir.exists():
            return

        loader = ViewLoader(views_dir)
        
        try:
            # 1. 发现并排序
            loader.discover_views()
            sorted_views = loader.get_sorted_views()
            
            # 2. 按顺序执行 SQL
            for view in sorted_views:
                try:
                    sql = view.get_sql(str(self.warehouse_dir))
                    conn.execute(sql)
                except Exception as e:
                    logger.error(f"加载视图失败 {view.name}: {e}")
                    
            logger.info(f"成功加载 {len(sorted_views)} 个视图")
            
        except Exception as e:
            logger.error(f"初始化视图层失败: {e}")

    def get_view_relationships_puml(self) -> str:
        """获取当前视图依赖关系的 PlantUML 源码"""
        views_dir = Path(__file__).parent / "views"
        loader = ViewLoader(views_dir)
        loader.discover_views()
        return loader.generate_puml()

    def list_available_views(self) -> List[str]:
        """获取当前 DuckDB 中可用的所有视图列表"""
        conn = self.get_duckdb_conn()
        res = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'").fetchall()
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
