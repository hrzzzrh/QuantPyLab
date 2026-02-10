import sqlite3
import duckdb
import os
from pathlib import Path
from config.settings import SQLITE_DB_PATH, WAREHOUSE_DIR
from typing import Optional

class DBManager:
    """
    数据库管理器，负责管理 SQLite (元数据) 和 DuckDB (分析数据) 的连接。
    遵循读写分离原则，统一通过此类获取连接。
    """
    
    def __init__(self):
        self.sqlite_path = SQLITE_DB_PATH
        
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
            # 彻底放弃物理文件，使用内存模式以实现完美的并发性
            self._duckdb_conn = duckdb.connect(":memory:")
            # 自动挂载 Parquet 数据湖中的视图
            self.init_warehouse_views(self._duckdb_conn)
        return self._duckdb_conn

    def init_warehouse_views(self, conn: duckdb.DuckDBPyConnection):
        """将 Parquet 目录映射为 DuckDB 视图，方便直接 SQL 查询"""
        view_map = {
            "fin_balance_sheet": "financial_statements/type=balance",
            "fin_income_statement": "financial_statements/type=income",
            "fin_cashflow_statement": "financial_statements/type=cashflow",
            "fin_indicator": "indicators",
            "fin_ttm": "financial/ttm",
            "daily_kline": "daily_kline",
            "share_capital": "share_capital"
        }
        
        for view_name, sub_dir in view_map.items():
            path = Path(WAREHOUSE_DIR) / sub_dir / "*/*.parquet"
            # 检查是否有文件存在，否则创建视图会报错
            if any(Path(WAREHOUSE_DIR).glob(f"{sub_dir}/*/data.parquet")):
                conn.execute(f"""
                    CREATE OR REPLACE VIEW {view_name} AS 
                    SELECT * FROM read_parquet('{path}', hive_partitioning=1, union_by_name=1)
                """)

        # 注册动态估值视图 (ASOF JOIN 核心逻辑)
        self._register_valuation_view(conn)

    def _register_valuation_view(self, conn: duckdb.DuckDBPyConnection):
        """
        注册核心估值视图：v_daily_valuation
        整合了：日线、复权因子、股本历史、财务 TTM、净资产。
        """
        # 检查依赖视图是否存在
        required_views = ["daily_kline", "share_capital", "fin_ttm", "fin_balance_sheet"]
        existing_views = [row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()]
        
        if not all(v in existing_views for v in required_views):
            return

        conn.execute("""
            CREATE OR REPLACE VIEW v_daily_valuation AS
            WITH 
            -- 1. 准备基础行情
            base_kline AS (
                SELECT symbol, CAST(date AS DATE) as date, close, adj_factor 
                FROM daily_kline
            ),
            -- 2. 准备股本历史
            capital_hist AS (
                SELECT symbol, CAST(change_date AS DATE) as change_date, total_shares 
                FROM share_capital
            ),
            -- 3. 准备财务 TTM 历史
            ttm_hist AS (
                SELECT symbol, strptime(pub_date, '%Y%m%d')::DATE as pub_date, net_profit_ttm, deduct_net_profit_ttm, revenue_ttm, ocf_ttm
                FROM fin_ttm
            ),
            -- 4. 准备净资产历史 (从资产负债表获取)
            assets_hist AS (
                SELECT 
                    symbol, 
                    -- 处理可能存在的多种日期格式 (YYYYMMDD 或 YYYY-MM-DD...)
                    CASE 
                        WHEN length(公告日期) = 8 THEN strptime(公告日期, '%Y%m%d')::DATE
                        ELSE CAST(LEFT(公告日期, 10) AS DATE)
                    END as pub_date,
                    "归属于母公司股东权益合计" as net_assets
                FROM fin_balance_sheet
            )

            SELECT 
                k.date,
                k.symbol,
                -- A. 价格指标
                k.close AS raw_close,
                (k.close * k.adj_factor) AS close_hfq, -- 后复权价格 (用于回测)
                
                -- B. 股本指标
                s.total_shares,
                
                -- C. 核心估值 (当时的总市值)
                (k.close * s.total_shares) AS market_cap,
                
                -- D. 估值比率 (ASOF JOIN 匹配当时已披露的数据)
                (k.close * s.total_shares) / NULLIF(t.net_profit_ttm, 0) AS pe_ttm,
                (k.close * s.total_shares) / NULLIF(t.deduct_net_profit_ttm, 0) AS pe_deduct_ttm,
                (k.close * s.total_shares) / NULLIF(a.net_assets, 0) AS pb,
                (k.close * s.total_shares) / NULLIF(t.revenue_ttm, 0) AS ps_ttm,
                (k.close * s.total_shares) / NULLIF(t.ocf_ttm, 0) AS pcf_ttm

            FROM base_kline k
            ASOF JOIN capital_hist s 
                ON k.symbol = s.symbol AND k.date >= s.change_date
            ASOF JOIN ttm_hist t 
                ON k.symbol = t.symbol AND k.date >= t.pub_date
            ASOF JOIN assets_hist a
                ON k.symbol = a.symbol AND k.date >= a.pub_date;
        """)

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
