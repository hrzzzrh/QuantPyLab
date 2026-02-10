# 项目操作手册

本文档记录 QuantPyLab 的所有运行指令及其参数说明。

### 1. 数据采集 (Data Ingestion)

#### 股票列表同步 (Basic Sync)
同步 A 股全量股票的基础代码与名称。
- **命令**: `uv run main.py --sync-stocks`
- **逻辑**: 
    1. 采用 **DELETE + append** 模式。
    2. 仅同步当前在市（Active）的股票。
- **建议频率**: 每周执行一次。
- **原因**: 保持数据库干净，仅包含当前可交易标的。
- **注意**: 历史退市股票目前不在同步范围内，`is_active` 字段对当前同步的股票默认均为 1。

#### 股票元数据补全 (Enrichment)
补全行业、地域、上市日期。
- **命令**: `uv run main.py --enrich-metadata [--industry] [--list-info] [--limit N]`
- **参数说明**:
    - `(无子参数)`: 默认执行 Stage 1 和 Stage 2。
    - `--industry`: 仅执行 Stage 1 (行业信息同步)。
    - `--list-info`: 仅执行 Stage 2 (地域与上市日期补全)。
- **策略**: 
    1. **Stage 1 (高速)**: 利用东财原生接口批量刷新行业信息。
    2. **Stage 2 (增量)**: 利用雪球和东财个股接口补全地域与上市日期。
- **建议频率**: 每次执行完 `--sync-stocks` 后执行，或每季度执行一次。
- **原因**: 行业分类和地域信息相对稳定，无需频繁更新。
- **性能**: 行业同步仅需约 1-2 分钟；详细信息补全视待处理数量而定（约 2-3 次请求/秒）。

### 财务报表同步
同步 A 股全量历史财务报表（资产负债表、利润表、现金流量表）。
- **命令**: `uv run main.py --sync-fin [--limit N] [--force-all]`
- **逻辑**: 
    1. **智能增量模式 (默认)**: 自动计算最近 4 个报告期，通过东财披露日历（沪深京全覆盖）识别已披露但本地缺失的股票进行抓取。
    2. **漏检补偿**: 自动识别从未同步过财报的“孤儿股”并优先同步。
    3. **强制全量**: 使用 `--force-all` 参数可忽略日历，强制检查全量股票的历史缺失情况。
    4. 采用单线程同步以保证 DuckDB Schema 动态扩展（ALTER TABLE）的安全性。
- **建议频率**: 每周执行一次 `uv run main.py --sync-fin` 即可自动追踪最新发布的财报。

### 财务指标同步 (Quant Indicators)
同步专业机构计算好的 140+ 个财务指标（如 ROE、毛利率、增长率等）。
- **命令**: `uv run main.py --sync-indicators [--limit N] [--symbol CODE] [--force-all]`
- **逻辑**: 
    1. **数据源**: 东方财富 (EM)。
    2. **存储规范**: 采用纯中文列名存储（基于 `em_indicator_dict.csv` 映射），列名经过规范化处理（去除单位、括号转下划线）。
    3. **智能增量模式 (默认)**: 与财报同步一致，通过披露日历识别最新发布的指标。
    4. **漏检补偿**: 自动扫描并补全从未同步过指标的股票。
    5. **手动模式**: 
       - `--symbol`: 强制同步单只。
       - `--force-all`: 全量扫描所有股票的指标缺失情况。
- **价值**: 为量化因子分析提供现成的、高阶的盈利与成长性数据，无需手动计算 YoY/QoQ。
- **建议频率**: 每周执行一次 `uv run main.py --sync-indicators`。

### 财务 TTM 指标计算
基于原始利润表与现金流量表，自主计算滚动十二个月 (Trailing Twelve Months) 指标。
- **命令**: `uv run main.py --calc-ttm [--limit N] [--symbol CODE] [--force-all]`
- **参数说明**:
    - `(无子参数)`: **智能增量模式 (默认)**。自动对比源报表与现有 TTM 库，识别新披露的报告期并触发补算，避免重复计算。
    - `--symbol`: 强制重新计算指定个股的所有历史 TTM。
    - `--force-all`: 强制全量扫描所有活跃股票并重新计算 TTM。
- **逻辑**: 
    1. **计算公式**: `TTM = 本期累计 + (上年年报 - 上年同期累计)`。
    2. **支持指标**: 
       - 归母净利润 TTM (`net_profit_ttm`)
       - 扣非净利润 TTM (`deduct_net_profit_ttm`)
       - 营业总收入 TTM (`revenue_ttm`)
       - 经营活动现金流 TTM (`ocf_ttm`)
    3. **时序安全**: 输出结果包含 `pub_date` (披露日)，确保在后续估值计算中无未来函数。
- **价值**: 为 PE (TTM)、PS (TTM) 等估值指标提供精准的分母数据。
- **建议频率**: 每次执行完 `--sync-fin` 或 `--sync-indicators` 后执行一次。

### 2. 数据分析与查询 (Analysis)

本项目采用 **Parquet Data Lake** 架构，数据以物理分片形式存储在 `data/warehouse/` 下。

#### 2.1 Python 接入 (推荐)
通过 `db_manager` 获取 DuckDB 连接，它会自动将所有 Parquet 文件映射为视图：
```python
from storage.database.manager import db_manager

# 获取连接 (瞬态内存模式)
conn = db_manager.get_duckdb_conn()

# 直接查询视图，操作体验与原物理表一致
df = conn.execute("SELECT * FROM fin_indicator WHERE symbol = '600519'").df()
print(df.head())
```

#### 2.2 外部工具接入 (DBeaver / DuckDB CLI)
由于没有物理 `.duckdb` 文件，在外部工具中请直接使用 `read_parquet` 函数查询目录：
```sql
-- 查询全量财务指标
SELECT * FROM read_parquet('data/warehouse/indicators/*/*.parquet', hive_partitioning=1);

-- 查询特定股票资产负债表
SELECT * FROM read_parquet('data/warehouse/financial_statements/type=balance/symbol=600519/*.parquet');
```

### 3. 环境维护
- **刷新 AI 上下文**: 在 Gemini CLI 中执行 `/memory refresh` 以加载最新的 `.gemini/GEMINI.md` 指令。
- **清理缓存**: (待添加)
