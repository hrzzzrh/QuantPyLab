# 历史行业点时数据资产设计

**设计日期**：2026-08-21
**状态**：已完成
**适用范围**：申万个股行业分类历史、统一视图和同步命令；暂不实现行业中性化

## 1. 背景与已验证事实

当前 `stocks.industry` 是雪球个股资料的当前快照，只有一个没有生效日期的字段，不能回答“某个历史信号日当时属于哪个行业”。因此，当前系统只能做点时规模暴露诊断，不能做历史行业暴露或行业中性化。

已对 AkShare 的 `stock_industry_clf_hist_sw()` 做了源码和原始文件核验：接口下载申万宏源研究的 `StockClassifyUse_stock.xls`，返回全市场个股行业变动历史，核心字段是股票代码、计入日期、行业代码和更新日期。2026-08-21 探测到 12,897 行、5,909 只股票，计入日期范围为 1990-01-01 至 2026-08-19；`symbol + effective_date` 没有重复，单股日期序列没有倒序。当前 `stocks` 表能匹配 5,879 只，另有 30 只历史代码不在当前元数据表中，不能因此丢弃。

来源与证据：

- AkShare 官方文档：`stock_industry_clf_hist_sw`，说明 `计入日期` 是行业分类生效日期，接口一次返回所有个股历史变动；
- AkShare 源码调用的原始文件：`https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls`；
- 本次验证已完成原始文件格式、行数、重复键、日期单调性和历史代码覆盖核验；验证结果记录在本设计文档和提交前测试中。

## 2. 数据口径

1. 分类标准固定为申万行业代码，不把雪球、东财、巨潮行业名称混入同一历史序列。
2. `effective_date` 使用原始“计入日期”，作为 ASOF 连接的生效日期；不能使用原始“更新日期”，后者只是数据源维护时间。
3. `industry_code` 保存为 6 位字符串，避免股票或行业代码的前导零被 Excel/NumPy 转为整数后丢失。
4. 按 `symbol + effective_date` 去重键校验；同一股票同一生效日出现多条不同分类时拒绝写入，不能静默选择一条。
5. 不要求历史代码必须存在于当前 `stocks` 表；退市股和元数据孤儿股仍应保留，以支持历史回测。
6. AkShare 已将原始“更新日期”标准化为日期，数据集字段命名为 `source_updated_date`；它只作为来源审计字段保留，绝不参与点时可见性判断。

## 3. 存储与视图

数据集使用 `industry_classification_sw`，按 `symbol=XXXXXX` 分区存储于 `data/warehouse/industry_classification_sw/`。每个分区保存：

| 字段 | 类型 | 含义 |
|:---|:---|:---|
| `effective_date` | DATE | 申万分类计入/生效日期 |
| `industry_code` | VARCHAR | 6 位申万行业代码 |
| `source_updated_date` | DATE | 申万源文件更新时间，仅用于审计 |

新增统一视图 `industry_classification_sw`，从分区路径提取 `symbol`。视图只负责暴露标准化数据，不把当前 `stocks.industry` 作为回退值。

后续行业点时查询固定为：

```sql
ASOF JOIN industry_classification_sw AS industry
  ON daily.symbol = industry.symbol
 AND daily.date >= industry.effective_date
```

按 `symbol, effective_date` 排序后取最近一条，信号日早于首条历史分类的股票保持行业缺失并计入覆盖率审计。

## 4. 同步协议

新增 `sync-industry-history` 命令，默认每天最多成功同步一次；`--force-refresh` 强制重新下载。流程为：下载全量文件 → 标准化列名和日期 → 校验代码、日期、重复键和排序 → 写入独立 staging 快照并完整 fsync → 在共享写锁内原子晋级整个数据集目录、清理旧分区 → 写入全局同步状态。源文件遗漏的旧股票不会残留在 canonical 目录，中途失败也不会产生新旧快照混合。

本阶段不自动覆盖 `stocks.industry`，也不把行业同步失败判定为财务或行情同步失败。数据资产稳定后，再将该同步环节纳入 `sync-all` 并增加调度失败策略。

## 5. 非目标

- 不在本阶段实现行业名称映射、行业收益因子或行业中性化约束；
- 不用当前行业快照填补历史缺失；
- 不用 `source_updated_at` 伪造历史可见日期；
- 不改变现有策略、训练、回测和规模暴露诊断行为。

## 6. 验收标准

- 纯函数测试覆盖列名标准化、前导零、日期解析、重复键、缺失字段、非六位行业代码和空响应；
- 视图测试覆盖 schema、分区路径和点时 ASOF 所需字段；
- CLI 同步测试 mock AkShare 和 Parquet 写入，不触真实网络或生产数据；
- 使用已下载原始样本完成一次真实格式/质量验证；
- 更新 `docs/` 数据目录和因子库边界文档；
- Ruff 检查、格式检查和全量 552 项测试已通过；提交前 Review Gate 通过后提交。
