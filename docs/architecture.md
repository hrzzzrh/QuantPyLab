# QuantPyLab 架构文档

## 核心技术栈
- **数据源**：[akshare](https://akshare.akfamily.xyz/) (核心数据获取库)
- **环境/依赖管理**：`uv`
- **存储方案**：
    - **Parquet Data Lake** (核心存储：存储财务报表、指标数据，按 `symbol` 分片存储，支持非阻塞并发读写)
    - **DuckDB** (计算引擎：通过 View 映射 Parquet 文件进行高性能列式查询)
    - **SQLite** (元数据数据库：存储配置、股票列表元信息、任务日志等)
- **分析工具**：Pandas, NumPy, Matplotlib/Plotly

## 数据源策略 (Data Sovereignty)
为确保数据的一致性与稳定性，系统实施严格的数据源分层策略：

| 数据层级 | 核心数据表 | 指定数据源 | 理由 |
| :--- | :--- | :--- | :--- |
| **原始报表层** | `fin_balance_sheet`, `fin_income_statement`, `fin_cashflow_statement` | **新浪财经 (Sina)** | 历史数据最全，包含账龄、附注等细节科目。 |
| **衍生指标层** | `fin_indicator` | **东方财富 (EastMoney)** | 计算标准统一，提供 YoY, QoQ, ROIC 等现成的高阶量化因子。**严禁与新浪指标混合**，以防计算口径冲突。 |
| **元数据层** | `stocks`, `industries` | **混合 (Mix)** | 东财提供行业分类，雪球提供实时行情状态。 |

## 目录架构说明
- `config/`: 存放股票列表、数据库路径、API 阈值等配置。
- `data_ingestion/`: 数据采集层。包含按类别划分的 `collectors` 和处理“历史+增量”逻辑的 `incremental.py`。
- `storage/`: 数据存储抽象层。
    - `database/`: DuckDB 与 SQLite 的连接管理及 SQL 脚本。
    - `file_store/`: Parquet 文件读写逻辑封装。
- `analysis/`: 策略与指标。包含自定义因子和技术指标计算。
- `backtest/`: 回测引擎。
- `utils/`: 工具函数（日志、时间处理、网络请求装饰器）。
- `data/`: 本地物理存储目录。

## 4. 标准扩展模式 (Extension Patterns)
新增数据功能时，必须遵循以下标准模式：

### 4.1 新增数据采集
1.  **Collector**: 在 `data_ingestion/collectors/` 新建类，负责 API 调用与清洗。
2.  **Store**: 在 `storage/database/` 新建存储类，负责 DuckDB/SQLite 写入（必须支持动态列扩展）。
3.  **Metadata**: 必须同步更新 `docs/data_catalog.md`，定义新字段的单位与含义。
4.  **Entry**: 在 `main.py` 注册相应的 `--sync-xxx` 命令。

### 4.2 开发生命周期
`docs/ (查阅现状)` -> `workspace/ (设计与Demo)` -> `Code (实现)` -> `docs/ (更新文档)`

## 5. 开发原则
1. **增量更新**：每次运行采集前先检查本地最新数据日期，仅抓取缺失部分。
2. **读写分离 (Atomic Write)**：数据写入采用“临时文件 + 原子重命名”模式。同步进程更新 Parquet 文件时，分析进程通过 DuckDB View 依然可以进行非阻塞读取。
3. **分析优先**：利用 DuckDB 的列式存储特性，大规模筛选（如选股、因子计算）应尽可能在 SQL 层完成。
4. **容错机制**：针对 `akshare` 接口，在 `utils` 中实现重试和限流。
5. **性能优先**：K 线数据优先使用 Parquet 存储。
