# 财务指标增量同步优化设计文档

## 1. 背景与目标 (Context)

当前 `sync_indicators` 功能采用简单的遍历全量同步模式：
- **低效**：无论数据是否更新，每次都重新抓取所有股票的所有历史指标。
- **资源浪费**：不仅消耗网络带宽，也增加了数据库的 I/O 压力（全量 Delete + Insert）。
- **目标**：对齐财务报表的同步逻辑，实现 **“披露驱动的增量同步 + 孤儿股补缺”** 机制。

## 2. 核心逻辑 (Core Logic)

新的同步流程将分为两个主要路径：

### 2.1 路径一：披露日历驱动 (Smart Sync)
这是日常运行的主要模式，用于及时捕获最新披露的数据。

1.  **计算目标报告期**：获取当前日期回溯的最近 4 个标准报告期（如 20241231, 20240930 等）。
2.  **获取披露计划**：调用 `ak.stock_yysj_em(date=...)` 获取全市场披露名单。
3.  **构建待同步集合**：
    *   筛选出 `actual_date` 不为空（已披露）的记录。
    *   查询数据库 `fin_indicator` 表，获取已存在的 `{symbol}_{report_date}` 集合。
    *   **Diff**：仅当 `{code}_{report_date}` 不在数据库中时，将该 `code` 加入待同步队列。

### 2.2 路径二：孤儿股补偿 (Orphan Cleanup)
用于处理新上市股票或历史遗漏的股票。

1.  **全量扫描**：获取 `stocks` 表中所有活跃股票代码。
2.  **库内比对**：查询 `fin_indicator` 表中所有已存在的 `symbol`。
3.  **差集计算**：找出从未同步过指标数据的“孤儿股”。
4.  **批次处理**：每次运行补偿一定数量（如 100 只），避免长时间阻塞。

## 3. 详细设计 (Detailed Design)

### 3.1 数据库层 (Storage Layer)

类：`storage/database/indicator_store.py:IndicatorStore`

需要新增/增强的方法：
- `get_existing_report_dates() -> set`: 
    - 返回格式：`{ "600000_20241231", "300001_20240930", ... }`
    - 用于快速 O(1) 查重。
- `get_stocks_without_indicators(all_codes: list) -> list`:
    - 找出 `fin_indicator` 表中完全不存在的 symbol。

### 3.2 采集层 (Ingestion Layer)

类：`data_ingestion/collectors/financial_collector.py:FinancialCollector`

现有方法 `collect_indicators` 逻辑确认：
- 接口：`ak.stock_financial_analysis_indicator_em`
- **关键特性验证**：该接口通常返回**全量历史数据**。
- **写入策略**：维持“逻辑 Upsert”策略。
    - 即使是增量同步，API 也会返回该股的所有历史数据。
    - Store 层依然执行：`DELETE FROM table WHERE symbol='...' AND report_date IN (...)` 然后 `INSERT`。
    - 这样可以顺便修复历史数据的潜在变动。

### 3.3 业务逻辑层 (Main Controller)

文件：`main.py` -> `sync_indicators`

重构步骤：
1.  不再直接遍历 `stocks` 表。
2.  **Step 1**: 调用 `get_target_report_dates` 计算关注的报告期。
3.  **Step 2**: 遍历报告期，获取 `FinancialCollector.get_disclosure_plans`。
4.  **Step 3**: 调用 `IndicatorStore.get_existing_report_dates` 获取已存状态。
5.  **Step 4**: 生成 `target_codes` 集合（Smart Sync）。
6.  **Step 5**: 调用 `IndicatorStore.get_stocks_without_indicators` 补充 `orphans`（Orphan Cleanup）。
7.  **Step 6**: 对 `target_codes` 进行遍历，调用 `collect_indicators`。

## 4. 验证计划 (Verification Plan)

### 4.1 接口行为验证 (Demo)
编写 `workspace/demo_indicator_check.py`，验证以下假设：
1.  `ak.stock_yysj_em` 能正确返回沪深京的披露时间。
2.  `ak.stock_financial_analysis_indicator_em` 返回的数据包含 `报告期` 字段，且格式可被标准化。

### 4.2 功能验证
1.  **空库测试**：运行脚本，应触发“孤儿股补偿”逻辑。
2.  **增量测试**：
    - 模拟数据库已有一部分数据。
    - 运行脚本，应只抓取最近披露但未入库的股票。
3.  **强制单只**：使用 `--symbol` 参数，确认仍可强制刷新单只股票。

## 5. 风险控制
- **接口限流**：东财接口较为宽松，但仍需保持 `time.sleep`。
- **数据一致性**：DuckDB 的 Delete+Insert 操作在单线程下是安全的，但需确保 Pandas DataFrame 的 `report_date` 格式与数据库中完全一致（`YYYYMMDD`）。
