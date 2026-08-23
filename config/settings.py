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

# 日志文件按天轮转, 保留最近 N 天的历史备份文件 (当天文件不计入)
LOG_RETENTION_DAYS = 30

# 数据获取配置
AKSHARE_TIMEOUT = 30
MAX_RETRIES = 3

# 日线行情只从该日期开始新增/重建；已有更早 canonical 数据不主动清理
MIN_KLINE_START_DATE = "20100101"

# 定时调度配置 (sync-all 流水线与同步状态分别控制重试)
# 流水线未全部成功时整体重试的最大次数 (总执行次数 = 1 + 此配置)
SYNC_ALL_MAX_RETRIES = 0
# sync_status SQLite 读写失败时的最大重试次数，避免瞬时锁冲突丢失状态
SYNC_STATUS_MAX_RETRIES = 3
# 重试间隔秒数
SYNC_ALL_RETRY_INTERVAL_SECONDS = 60
