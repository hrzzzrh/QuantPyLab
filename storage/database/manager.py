import sqlite3
import duckdb
import os
from config.settings import SQLITE_DB_PATH, DUCKDB_PATH
from typing import Optional

class DBManager:
    """
    数据库管理器，负责管理 SQLite (元数据) 和 DuckDB (分析数据) 的连接。
    遵循读写分离原则，统一通过此类获取连接。
    """
    
    def __init__(self):
        self.sqlite_path = SQLITE_DB_PATH
        self.duckdb_path = DUCKDB_PATH
        
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
        """获取 DuckDB 连接 (分析数据)"""
        if self._duckdb_conn is None:
            self._duckdb_conn = duckdb.connect(str(self.duckdb_path))
        return self._duckdb_conn

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
