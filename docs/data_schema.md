# 数据模型规范

## SQLite 元数据库 (metadata.db)

### 1. stocks 表
存储 A 股全量股票的基础索引信息。

| 字段 | 类型 | 约束 | 说明 |
| :--- | :--- | :--- | :--- |
| `symbol` | TEXT | PRIMARY KEY | 带市场前缀的代码 (如 sh600000) |
| `code` | TEXT | NOT NULL | 纯代码 (如 600000) |
| `name` | TEXT | NOT NULL | 股票名称 |
| `area` | TEXT | - | 地域 (待补充) |
| `industry` | TEXT | - | 细分行业 (待补充) |
| `list_date` | TEXT | - | 上市日期 (YYYYMMDD, 待补充) |
| `is_active` | INTEGER | DEFAULT 1 | 是否在交易 |
| `updated_at` | DATETIME | DEFAULT CURRENT_TIMESTAMP | 最后同步时间 |

**同步策略**：
- **Phase 1 (基础)**：使用 `akshare.stock_info_a_code_name` 建立代码索引。
- **Phase 1.5 (增强)**：
    - **行业**：通过东财原生批量接口（Native API）高效同步。
    - **地域/上市日期**：通过雪球个股接口（XQ）与东财个股接口（EM）混合增量补全。
- **写入规范**：基础同步必须先 `DELETE` 后 `append`；增强同步使用 `UPDATE`。严禁使用 `to_sql(replace)`。

## DuckDB 分析数据库 (analysis.duckdb)

### 1. 财务报表 (fin_balance_sheet, fin_income_statement, fin_cashflow_statement)
存储 A 股三大财务报表的原始数据。

**核心共有字段**：
- `symbol`: 股票代码 (6位)
- `report_date`: 报告期 (YYYYMMDD)
- `ann_date`: 公告日期
- `is_audited`: 是否审计
- (其他字段): 对应新浪财经原始中文指标名

**同步策略**：
- **驱动机制**：默认采用“披露日历驱动”的智能增量模式，通过聚合沪深京全市场披露计划识别待更新标的。
- **兜底机制**：自动扫描并优先补全从未入库的“孤儿股”。
- **数据源**：新浪财经。
- **写入规范**：采用“逻辑 Upsert”模式（先删除该股同报告期旧数据，再插入新数据），支持动态列扩展。

**特征**：
- **宽表存储**：采用动态列扩展模式，不同行业的特有指标会自动增加为新列。
- **数据类型**：指标列统一为 `DOUBLE`，元数据列为 `VARCHAR`。
- **唯一性**：通过逻辑层确保 `(symbol, report_date)` 唯一。
