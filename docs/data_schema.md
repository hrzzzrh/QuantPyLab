# 数据模型规范

## 1. 文档索引
**字段详细定义**：关于所有表（基础行情、财务报表、财务指标、TTM 数据）的具体字段含义、单位及样例值，请务必查阅 **[数据资产目录 (Data Catalog)](data_catalog.md)**。

## 2. SQLite 元数据库 (data/metadata.db)

SQLite 在本项目中充当 **元数据注册表 (Registry)**，物理文件存储于 `data/metadata.db`。它负责维护低频变动、强关联性的基础索引数据，是系统启动、任务分发及物理分片定位的**事实来源 (Source of Truth)**。

### A. 职责范围
- **实体索引**: 定义全市场合法股票的 `symbol` 与 `code` 映射。
- **静态属性**: 存储行业、地域、上市日期等用于分组过滤的静态维度。
- **状态维护**: 记录股票的存续状态 (`is_active`)，指导增量同步引擎排除已退市个股。

### B. 核心表: `stocks` (全量股票索引)
所有数据同步任务（Parquet 分片）均依赖于此表的 `symbol` 字段。

| 字段名 | 类型 | 约束 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- |
| `symbol` | TEXT | PRIMARY KEY | 6 位纯数字代码 (物理分片键) | `600519`, `000001` |
| `code` | TEXT | NOT NULL | 纯 6 位数字代码 | `600519` |
| `name` | TEXT | NOT NULL | 股票简称 | `贵州茅台` |
| `area` | TEXT | - | 地域 (省份/城市) | `贵州` |
| `industry` | TEXT | - | 东财细分行业 | `白酒` |
| `list_date` | TEXT | - | 上市日期 (格式: YYYYMMDD) | `20010827` |
| `is_active` | INTEGER | DEFAULT 1 | 存续状态 (1:在市, 0:退市) | `1` |
| `updated_at` | DATETIME | DEFAULT ... | 最后同步时间 (UTC) | `2026-02-22 10:00:00` |

### C. 同步逻辑与维护
- **全量重建**: 执行 `sync-stocks` 时，系统会清空并重新从 AkShare 获取最新代码列表。
- **属性补全**: 执行 `sync-metadata` 时，系统会针对 `area`, `industry`, `list_date` 等空字段，通过多源（雪球/东财）进行异步并发补全。

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
