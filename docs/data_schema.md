# 数据模型规范

## 1. 文档索引
**字段详细定义**：关于所有表（基础行情、财务报表、财务指标、TTM 数据）的具体字段含义、单位及样例值，请务必查阅 **[数据资产目录 (Data Catalog)](data_catalog.md)**。

## 2. SQLite 元数据库 (metadata.db)
... (此处保留 stocks 表原有规范)

## 3. DuckDB 统一视图层 (Unified View Architecture)

本项目采用“**物理存储与代码定义视图**”的架构。DuckDB 中所有可查询对象均为 **视图 (View)**，其定义采用 Python 类进行声明式管理。

### A. 物理存储层 (Data Lake)
物理数据以 Parquet 格式存储在 `data/warehouse/` 中。物理文件不直接暴露给分析逻辑，必须通过视图访问。

### B. 视图定义规范 (Code-as-Definition)
所有视图定义位于 `storage/database/views/` 目录下，每个视图对应一个 Python 文件。

**核心基类**: `storage.database.view_base.DuckDBView`

| 特性 | 说明 |
| :--- | :--- |
| **显式依赖** | 必须在 `dependencies` 属性中列出所依赖的视图名。 |
| **自动化加载** | 系统在启动时会自动构建有向无环图 (DAG) 并按正确顺序加载视图。 |
| **动态 SQL** | 可以在 Python 中利用逻辑动态生成 SQL 语句（如路径替换、字段筛选）。 |

**目录结构即业务域 (Domain)**：
- `market/`: 基础行情映射视图。
- `financial/`: 财务报表与指标映射视图。
- `analysis/`: 衍生分析视图（如估值、因子）。

### C. 设计原则
- **无穿越保障**：分析视图必须使用 `ASOF JOIN` 关联财务数据，且关联键必须为 `k.date >= f.pub_date`。
- **自动可视化**：系统可根据 Python 类定义自动生成 PlantUML 关系图，确保架构透明。
