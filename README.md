# QuantPyLab

基于 Python 的 A 股量化交易实验室，实现数据采集、分析与回测全流程。

## 快速开始

### 1. 环境准备
项目使用 `uv` 进行包管理。
```bash
# 安装依赖
uv sync
```

### 2. 数据初始化
同步全量 A 股股票列表元数据：
```bash
uv run main.py --sync-stocks
```

补全股票详细元数据（行业、地域、上市日期）：
```bash
uv run main.py --enrich-metadata
```

同步 A 股全量财务报表（三大表）：
```bash
# 智能增量同步 (披露驱动)
uv run main.py --sync-fin

# 强制全量扫描
uv run main.py --sync-fin --force-all
```

同步 A 股财务指标（ROE、增长率、周转率等）：
```bash
# 智能增量同步 (披露驱动)
uv run main.py --sync-indicators

# 指定同步单只股票
uv run main.py --sync-indicators --symbol 300274
```

## 项目结构
- `investigation/`: 投研产出中心（个股跟踪手册、行业研究、宏观报告）。
- `docs/`: 系统架构、数据字典与操作手册。
- `workspace/`: 工程实验室（开发草案、分析脚本、原始数据）。
- `data_ingestion/`: 数据采集引擎。
- `storage/`: 存储与计算引擎（Parquet + DuckDB）。
- `analysis/`: 因子库与分析逻辑。
- `data/`: 本地数据仓库。
详细架构请参阅 `docs/architecture.md`。
