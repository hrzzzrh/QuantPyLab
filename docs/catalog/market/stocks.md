# 股票基础信息表 (stocks)

存储 A 股全量股票的基础索引信息，位于 `metadata.db` (SQLite)。

## 1. 字段定义

| 序号 | 字段名 | 类型 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `symbol` | TEXT | 带市场前缀的代码 (主键) | sh600519 |
| 2 | `code` | TEXT | 纯 6 位代码 | 600519 |
| 3 | `name` | TEXT | 股票名称 | 贵州茅台 |
| 4 | `area` | TEXT | 地域 (省份/城市) | 贵州 |
| 5 | `industry` | TEXT | 东财细分行业 | 白酒 |
| 6 | `list_date` | TEXT | 上市日期 (YYYYMMDD) | 20010827 |
| 7 | `is_active` | INTEGER | 是否在交易 (1:在市, 0:退市) | 1 |
| 8 | `updated_at` | DATETIME | 最后同步时间 | 2026-02-08 20:00:00 |
