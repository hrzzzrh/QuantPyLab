# 股本变动表 (share_capital)

记录股票自上市以来的所有股本变更记录。

## 1. 字段定义

| 序号 | 字段名 | 类型 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `change_date` | DATE | 股本变动日期 | 2009-12-31 |
| 2 | `total_shares` | DOUBLE | 总股本 (股)。注：同步代码尝试写入 BIGINT，但实际分片类型为 DOUBLE | 943800000 |
| 3 | `symbol` | VARCHAR | 股票代码 (纯数字，如 600519)。物理文件位于 `symbol={code}` 分区目录，视图层通过文件名提取 | 600519 |

## 2. 数据来源
- 接口: 新浪财经 (网页爬取)
- 逻辑: 解析新浪财经股本变动历史页面，获取全量历史变动记录，并在本地执行增量过滤 (仅保存本地最新 `change_date` 之后的数据)。
- URL: `https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructureHistory/stockid/{symbol}/stocktype/TotalStock.phtml`

## 3. 同步状态
- 每只股票同步成功后在 `metadata.db` 的 `sync_status` 表记录当次同步日期 (`dataset='share_capital'`)。
- 批量同步 (无 `--symbol`/`--force-all`) 时，当日已同步成功的股票自动跳过；失败股票无记录，重跑自动补抓。
- 同步失败 (重试后仍异常) 不写入 `sync_status`，留待下次重跑。
