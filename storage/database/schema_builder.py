import json
from datetime import datetime
from pathlib import Path

import duckdb

from config.settings import WAREHOUSE_DIR
from utils.logger import logger

# 数据集 → Parquet glob 模式 (相对 warehouse 根目录)
DATASET_PATTERNS = {
    "daily_kline": "daily_kline/*/*.parquet",
    "etf_kline": "etf_kline/*/*.parquet",
    "share_capital": "share_capital/*/*.parquet",
    "fin_ttm": "financial/ttm/*/*.parquet",
    "fin_balance_sheet": "financial_statements/type=balance/*/*.parquet",
    "fin_income_statement": "financial_statements/type=income/*/*.parquet",
    "fin_cashflow_statement": "financial_statements/type=cashflow/*/*.parquet",
    "fin_indicator": "indicators/*/*.parquet",
}

SCHEMA_DIR = Path(__file__).parent / "views" / "schemas"

# 类型加宽优先级: 取值越高越宽
_TYPE_RANK = {"VARCHAR": 0, "INTEGER": 1, "BIGINT": 2, "FLOAT": 3, "DOUBLE": 4}


def _wider_type(t1: str, t2: str) -> str:
    """选择更宽的类型 (避免精度损失)"""
    return t1 if _TYPE_RANK.get(t1, 5) >= _TYPE_RANK.get(t2, 5) else t2


def build_schema(pattern: str) -> dict[str, str]:
    """
    扫描数据集全部分片，聚合列名+类型的并集。
    同名不同型取更宽类型。使用 parquet_schema 单次 SQL 完成，无需逐文件打开。
    """
    conn = duckdb.connect(":memory:")
    full_pattern = f"{WAREHOUSE_DIR}/{pattern}"
    rows = conn.execute(
        """
        SELECT name, duckdb_type, count(DISTINCT file_name) as nfiles
        FROM parquet_schema(?)
        WHERE name NOT IN ('schema_id', 'file_row_number') AND num_children IS NULL
        GROUP BY name, duckdb_type
        """,
        [full_pattern],
    ).fetchall()
    conn.close()

    types: dict[str, str] = {}
    total_files = 0
    for name, dtype, nfiles in rows:
        total_files = max(total_files, nfiles)
        dt = dtype.upper() if dtype else "VARCHAR"
        if name not in types:
            types[name] = dt
        else:
            types[name] = _wider_type(types[name], dt)

    if not types:
        raise ValueError(f"数据集无可用 schema: {pattern}")
    return types


def save_schema(dataset: str, schema: dict[str, str]):
    """写入 schema 缓存 JSON"""
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "built_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "column_count": len(schema),
        "columns": schema,
    }
    path = SCHEMA_DIR / f"{dataset}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"schema 缓存已写入: {path} ({len(schema)} 列)")


def load_schema(dataset: str) -> dict[str, str] | None:
    """读取 schema 缓存；缺失/损坏返回 None"""
    path = SCHEMA_DIR / f"{dataset}.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        cols = payload.get("columns")
        if not isinstance(cols, dict) or not cols:
            return None
        return cols
    except (json.JSONDecodeError, OSError):
        return None


def ensure_schema(dataset: str) -> dict[str, str]:
    """
    获取数据集 schema，缺失/损坏时自动重建。
    返回 (schema, rebuilt 标记) 之外的 schema 字典。
    """
    cached = load_schema(dataset)
    if cached is not None:
        return cached
    pattern = DATASET_PATTERNS.get(dataset)
    if pattern is None:
        raise ValueError(f"未知数据集: {dataset}")
    logger.warning(f"schema 缓存缺失，自动重建: {dataset}")
    schema = build_schema(pattern)
    save_schema(dataset, schema)
    return schema


def rebuild_dataset(dataset: str):
    """重建单个数据集的 schema 缓存"""
    pattern = DATASET_PATTERNS.get(dataset)
    if pattern is None:
        raise ValueError(f"未知数据集: {dataset}")
    schema = build_schema(pattern)
    save_schema(dataset, schema)


def rebuild_all():
    """重建全部数据集的 schema 缓存"""
    for dataset in DATASET_PATTERNS:
        try:
            rebuild_dataset(dataset)
        except Exception:
            logger.exception(f"重建 schema 失败: {dataset}")
