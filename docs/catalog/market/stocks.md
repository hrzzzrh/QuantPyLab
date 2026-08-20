# 股票基础信息表 (stocks)

存储 A 股全量股票的基础索引信息，位于 `metadata.db` (SQLite)。

`industry` 是雪球个股资料的当前行业快照，没有历史生效日期，不能用于历史回测的行业暴露或行业中性化。历史点时行业分类使用独立数据集 `industry_classification_sw`。

## 1. 字段定义

| 序号 | 字段名 | 类型 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `symbol` | TEXT | 带市场前缀的代码 (主键) | sh600519 |
| 2 | `code` | TEXT | 纯 6 位代码 | 600519 |
| 3 | `name` | TEXT | 股票名称 | 贵州茅台 |
| 4 | `area` | TEXT | 地域 (省份/城市) | 贵州 |
| 5 | `industry` | TEXT | 行业 (雪球资料源) | 白酒 |
| 6 | `list_date` | TEXT | 上市日期 (YYYYMMDD) | 20010827 |
| 7 | `is_active` | INTEGER | 是否在交易 (1:在市, 0:退市) | 1 |
| 8 | `last_trade_date` | TEXT | 最后交易日 (YYYYMMDD)，由退市股 K 线新浪 KLC 重建或腾讯整股 fallback 流程写入真实值；未重建前为 NULL | 20260630 |
| 9 | `updated_at` | DATETIME | 最后同步时间 | 2026-02-08 20:00:00 |

## 2. 同步方式

执行 `sync-stocks` 时以 AkShare 当前在市列表为基准做**差量 diff 更新**（非清空重建）：

- 新上市股票 → 插入 (is_active=1)
- 已在市股票 → 更新名称，若曾被误标退市则恢复
- 从列表消失的股票 → 标记退市 (is_active=0)；`last_trade_date` 由退市股 K 线新浪 KLC 重建或腾讯整股 fallback 流程写入真实最后交易日
- 接口返回空列表时跳过本次更新，防止误标退市

### 2.1 退市股清单合并
diff 完成后，从沪深交易所退市股列表接口 (AkShare `stock_info_sh_delist` / `stock_info_sz_delist`) 合并**历史退市股清单**：

- **必要性**: 当前在市列表不含历史退市股，仅靠 diff 无法恢复退市股 (重建场景 stocks 表将缺失退市股，其数据永远无法同步)
- 逻辑: 清单中 stocks 表不存在的代码 → 插入 (is_active=0, `last_trade_date` 为接口日期初值)；**不修改已有行** (保留 K 线重建写入的真实最后交易日)
- 覆盖: 沪深退市股约 360 只 (沪 ~153 + 深 ~208)；北交所退市股无清单接口，暂不覆盖
- 防误标: 接口异常/返回空时跳过该步
