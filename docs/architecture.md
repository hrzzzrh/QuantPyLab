# QuantPyLab 架构文档

## 核心技术栈
- **数据源**：[akshare](https://akshare.akfamily.xyz/) (核心数据获取库)
- **环境/依赖管理**：`uv`
- **存储方案**：
    - **DuckDB** (核心分析数据库：存储财务报表、基本面数据及计算因子，支持高性能列式查询)
    - **SQLite** (元数据数据库：存储配置、股票列表元信息、任务日志等)
    - **文件系统**：Parquet 格式 (存储日线/分钟线 K 线，可被 DuckDB 直接读取)
- **分析工具**：Pandas, NumPy, Matplotlib/Plotly

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

## 开发原则
1. **增量更新**：每次运行采集前先检查本地最新数据日期，仅抓取缺失部分。
2. **读写分离**：统一通过 `storage/manager.py` 接口读写数据，不直接在业务层操作 SQL/文件。
3. **分析优先**：利用 DuckDB 的列式存储特性，大规模筛选（如选股、因子计算）应尽可能在 SQL 层完成。
4. **容错机制**：针对 `akshare` 接口，在 `utils` 中实现重试和限流。
5. **性能优先**：K 线数据优先使用 Parquet 存储。
