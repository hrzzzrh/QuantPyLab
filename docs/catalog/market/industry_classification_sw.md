# 申万历史行业分类数据资产 (industry_classification_sw)

## 1. 数据集与来源

数据集名称：industry_classification_sw。物理数据按股票分区存储于 data/warehouse/industry_classification_sw/symbol=XXXXXX/data.parquet，统一视图名称为 industry_classification_sw。

来源为 AkShare stock_industry_clf_hist_sw() 封装的申万宏源研究历史分类文件。该接口返回个股行业分类的变动历史；本系统使用“计入日期”作为行业分类的生效日期，不使用“更新日期”做历史可见性判断。

## 2. 字段定义

| 字段名 | 类型 | 说明 |
|:---|:---|:---|
| symbol | VARCHAR | 从 Hive 分区路径提取的 6 位股票代码 |
| effective_date | DATE | 申万行业分类计入/生效日期 |
| industry_code | VARCHAR | 6 位申万行业代码 |
| source_updated_date | DATE | 来源文件更新日期，仅用于审计 |

同一 symbol + effective_date 必须唯一。历史代码即使不在当前 stocks 快照中也保留，避免退市股历史回测丢失行业信息。

## 3. 点时使用规范

历史行业只能按生效日期 ASOF 连接：

    ASOF JOIN industry_classification_sw AS industry
      ON daily.symbol = industry.symbol
     AND daily.date >= industry.effective_date

信号日早于该股票首条分类记录时，行业值保持缺失并计入覆盖率；严禁用当前 stocks.industry 回填历史行业。

source_updated_date 是来源维护时间，不是行业分类生效时间，不能用于 ASOF 条件。

## 4. 同步方式

    uv run main.py sync-industry-history
    uv run main.py sync-industry-history --force-refresh

同步流程会校验字段、日期、代码长度、重复键后按股票分区写入；默认当天已成功同步时跳过。该命令目前独立于 sync-all，行业历史数据稳定后再纳入统一调度。
