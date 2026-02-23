# 股本变动表 (share_capital)

记录股票自上市以来的所有股本变更记录。

## 1. 字段定义

| 序号 | 字段名 | 类型 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `change_date` | DATE | 股本变动日期 | 2009-12-31 |
| 2 | `total_shares` | BIGINT | 总股本 (股) | 943800000 |

## 2. 数据来源
- 接口: 新浪财经 (网页爬取)
- 逻辑: 解析新浪财经股本变动历史页面，获取全量历史变动记录，并在本地执行增量过滤。
- URL: `https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructureHistory/stockid/{symbol}/stocktype/TotalStock.phtml`
