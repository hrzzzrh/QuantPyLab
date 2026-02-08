import os
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 数据存储目录
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 数据库路径
SQLITE_DB_PATH = DATA_DIR / "metadata.db"

# Parquet 数据仓路径
WAREHOUSE_DIR = DATA_DIR / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# 日志配置
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 数据获取配置
AKSHARE_TIMEOUT = 30
MAX_RETRIES = 3
