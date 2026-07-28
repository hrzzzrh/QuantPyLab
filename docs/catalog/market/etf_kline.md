# ETF日线行情表 (etf_kline)

存储ETF基金的每日原始价格与成交数据，以 Parquet 格式存储在数据湖中。

## 1. 字段定义

| 序号 | 字段名 | 类型 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `symbol` | VARCHAR | ETF代码 (纯数字，分区键) | 510050 |
| 2 | `date` | DATE | 交易日期 | 2026-05-17 |
| 3 | `open` | DOUBLE | **不复权**开盘价 | 3.020 |
| 4 | `high` | DOUBLE | **不复权**最高价 | 3.030 |
| 5 | `low` | DOUBLE | **不复权**最低价 | 2.992 |
| 6 | `close` | DOUBLE | **不复权**收盘价 | 3.008 |
| 7 | `volume` | DOUBLE | 成交量 (股) | 870984563 |
| 8 | `amount` | DOUBLE | 成交额 (元) | 2621819056.0 |
| 9 | `adj_factor` | DOUBLE | **后复权因子** (用于计算后复权价) | 1.447374 |

## 2. 数据来源

- **接口**: 雪球K线API (`https://stock.xueqiu.com/v5/stock/chart/kline.json`)
- **逻辑**: 
  - 调用两次API获取不复权 (`type=normal`) 和后复权 (`type=after`) 数据
  - 计算复权因子: `adj_factor = close_hfq / close_normal`
  - 支持增量同步

## 3. 分红处理

ETF基金也会分红（如上证50ETF累计分红0.797元），因此需要复权处理：
- **不复权价格**: 用于查看真实历史价格
- **后复权价格**: 用于计算真实收益率（复权价格 = 不复权价格 × 复权因子）

## 4. 存储路径

```
data/warehouse/etf_kline/symbol={symbol}/data.parquet
```

示例：
```
data/warehouse/etf_kline/symbol=510050/data.parquet
data/warehouse/etf_kline/symbol=159915/data.parquet
```
