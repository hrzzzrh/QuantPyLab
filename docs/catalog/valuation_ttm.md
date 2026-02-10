# 估值 TTM 数据资产 (Valuation TTM)

## 表名: fin_ttm
存储自主计算的滚动十二个月 (Trailing Twelve Months) 财务指标，用于历史估值计算。

| 序号 | 字段名 | 类型 | 单位 | 备注 | 样例值 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | report_date | VARCHAR | - | 报告期 (YYYYMMDD) | 20250930 |
| 2 | pub_date | VARCHAR | - | 披露日期 (YYYYMMDD)，**用于防止未来函数** | 20251030 |
| 3 | net_profit_ttm | DOUBLE | 元 | 归属于母公司所有者的净利润 (TTM) | 90027340000.0 |
| 4 | deduct_net_profit_ttm | DOUBLE | 元 | 扣除非经常性损益后的归母净利润 (TTM) | 90142900000.0 |
| 5 | revenue_ttm | DOUBLE | 元 | 营业总收入 (TTM) | 181925400000.0 |
| 6 | ocf_ttm | DOUBLE | 元 | 经营活动产生的现金流量净额 (TTM) | 86239110000.0 |
| 7 | symbol | VARCHAR | - | 股票代码 | 600519 |
