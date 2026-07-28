# ETF基金基础信息表 (etfs)

存储场内交易基金的基础索引信息，以 SQLite 表形式存储在元数据库中。

## 1. 字段定义

| 序号 | 字段名 | 类型 | 约束 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `symbol` | TEXT | PRIMARY KEY | 6位纯数字代码 | 510050 |
| 2 | `code` | TEXT | NOT NULL | 6位纯数字代码 | 510050 |
| 3 | `name` | TEXT | NOT NULL | ETF简称 | 上证50ETF华夏 |
| 4 | `fund_type` | TEXT | - | 基金类型 | 指数型-股票 |
| 5 | `list_date` | TEXT | - | 成立日期 (格式: YYYYMMDD) | 20040102 |
| 6 | `is_active` | INTEGER | DEFAULT 1 | 存续状态 (1:在市, 0:退市) | 1 |
| 7 | `updated_at` | DATETIME | DEFAULT ... | 最后同步时间 (UTC) | 2026-05-18 22:49:33 |

## 2. 数据来源

- **接口**: `ak.fund_exchange_rank_em()`
- **说明**: 东方财富场内交易基金排行榜，包含全量ETF基金列表

## 3. 同步策略

- **全量重建**: 每次执行 `sync-etf-list` 时，清空并重新从东财获取最新ETF列表
- **频率**: 按需同步，ETF列表变化频率较低
