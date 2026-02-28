# QuantPyLab

`QuantPyLab` 是一个基于 Python 的量化交易实验室，旨在实现从 A 股数据获取、分片存储、高性能计算到深度投研的全流程闭环。

## 🚀 核心特性

- **存算分离架构**：采用 **Parquet Data Lake** 作为冷存储，**DuckDB** 作为高性能 OLAP 计算引擎。
- **统一视图层**：基于 `Code-as-Definition` 模式，通过 SQL 视图统一屏蔽 Parquet 文件分片细节。
- **智能增量同步**：由披露日历驱动，支持财务报表、指标、K 线及股本变动的全自动增量采集。
- **无穿越 TTM 引擎**：自主实现滚动十二个月（TTM）财务指标计算，确保回测无未来函数。
- **工程化投研**：集成个股跟踪手册与研究报告体系，实现“数据-分析-结论”的价值沉淀。

## 🛠️ 快速开始

### 1. 环境准备
项目使用 `uv` 进行高效包管理。
```bash
# 安装依赖
uv sync
```

### 2. 初始化元数据
同步全量 A 股列表及行业、地域、上市日期等关键元信息：
```bash
# 同步股票列表 (SQLite)
uv run main.py sync-stocks

# 补全行业、地域、上市日期
uv run main.py sync-metadata
```

### 3. 数据同步流水线
系统支持单指令全量同步，也可针对特定数据源进行精细化操作：
```bash
# 【推荐】一键同步全量数据 (财务、指标、TTM、股本、K线)
uv run main.py sync-all

# 单独同步日线行情 (自动计算复权)
uv run main.py sync-kline --symbol 600519

# 计算 TTM 财务指标
uv run main.py calc-ttm
```

## 📂 项目结构

```text
├── main.py                # 统一命令行入口 (CLI)
├── analysis/              # 指令/因子库与 TTM 计算引擎
├── data_ingestion/        # 数据采集层 (支持新浪、东财、雪球等)
├── storage/               # 存储与计算层 (DuckDB/SQLite/Parquet)
│   └── database/views/    # 统一视图定义 (Code-as-Definition)
├── investigation/         # 投研产出中心 (个股跟踪手册、行业深度)
├── docs/                  # 系统架构、数据字典与操作手册
├── workspace/             # 工程实验室 (草案脚本、临时实验)
└── data/warehouse/        # 本地数据湖 (Parquet 分片存储)
```

## 📊 数据资产概览

| 类别 | 存储格式 | 核心表/视图 | 频率 |
| :--- | :--- | :--- | :--- |
| **元数据** | SQLite | `stocks` | 不定期 |
| **财务报表** | Parquet | `fin_balance_sheet`, `fin_income_statement`, `fin_cashflow` | 季度 (披露驱动) |
| **财务指标** | Parquet | `fin_indicator` (140+ 维度) | 季度 |
| **滚动指标** | Parquet | `fin_ttm` (无穿越 TTM 数据) | 计算生成 |
| **日线行情** | Parquet | `market_daily_kline` | 每日 |
| **股本变动** | Parquet | `share_capital` | 实时/定期 |

## 📐 开发原则

1.  **文档先行**：复杂变更需先在 `workspace/` 编写设计文档。
2.  **视图优先**：所有业务查询必须通过 `storage/database/views/` 下定义的视图完成。
3.  **命名精准**：CLI 指令遵循 `动作-对象` 规范（如 `sync-kline` 而非 `kline`）。
4.  **实测验证**：针对新接口必须在 `workspace/` 进行采样测试后再合入主干。

---
*更多详细文档请参阅 `docs/` 目录。*
