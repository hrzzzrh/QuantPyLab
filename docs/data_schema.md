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
