# 估值分析视图资产 (Valuation Views)

## 视图名称: v_daily_valuation
该视图是系统的核心分析入口，通过 `ASOF JOIN` 动态对齐了行情与财务数据，确保在任何历史时间点看到的都是“当时已披露”的最准确信息。

### 1. 字段定义

| 序号 | 字段名 | 类型 | 说明 | 逻辑/来源 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `date` | DATE | 交易日期 | `daily_kline.date` |
| 2 | `symbol` | VARCHAR | 股票代码 | `daily_kline.symbol` |
| 3 | `raw_close` | DOUBLE | 不复权收盘价 | `daily_kline.close` |
| 4 | `close_hfq` | DOUBLE | **后复权**收盘价 | `close * adj_factor` |
| 5 | `total_shares` | DOUBLE | 当日总股本 | 匹配 `share_capital` 最新变动 |
| 6 | `market_cap` | DOUBLE | 总市值 | `raw_close * total_shares` |
| 7 | `pe_ttm` | DOUBLE | 市盈率 (TTM) | `market_cap / net_profit_ttm` |
| 8 | `pe_deduct_ttm` | DOUBLE | 扣非市盈率 (TTM) | `market_cap / deduct_net_profit_ttm` |
| 9 | `pb` | DOUBLE | 市净率 | `market_cap / net_assets` |
| 10 | `ps_ttm` | DOUBLE | 市销率 (TTM) | `market_cap / revenue_ttm` |
| 11 | `pcf_ttm` | DOUBLE | 市现率 (TTM) | `market_cap / ocf_ttm` |

### 2. 核心关联逻辑
视图采用 **Point-in-Time** 对齐算法：
1. **行情与股本**：按 `symbol` 分组，找到 `date >= change_date` 的最新一条股本记录。
2. **行情与财务**：按 `symbol` 分组，找到 `date >= pub_date` 的最新一条财务 TTM 记录（确保使用公告日，规避未来函数）。

### 3. 使用样例
```sql
-- 查询贵州茅台在 2024 年全年的估值走势
SELECT date, pe_ttm, pb, ps_ttm 
FROM v_daily_valuation 
WHERE symbol = '600519' 
  AND date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY date;
```
