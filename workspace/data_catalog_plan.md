# Data Catalog 建设规划 (Metastore Construction)

## 1. 目标
在 `analysis.duckdb` 中建立 `data_catalog` 表，作为全系统数据的“说明书”。它必须回答：
- 这个字段 (`column_name`) 属于哪张表 (`table_name`)？
- 它的原始全名是什么 (`original_name`)？
- 它的单位是什么 (`unit`)？
- 它的业务分类是什么 (`category`)？

## 2. 表结构设计
**表名**: `data_catalog`

| 字段 | 类型 | 说明 | 示例 |
| :--- | :--- | :--- | :--- |
| `id` | VARCHAR | 唯一标识 (Table.Col) | `fin_indicator.净资产收益率_加权` |
| `table_name` | VARCHAR | 所属表名 | `fin_indicator` |
| `column_name` | VARCHAR | 实际存储列名 | `净资产收益率_加权` |
| `display_name` | VARCHAR | 原始/展示名称 | `净资产收益率(加权)` |
| `unit` | VARCHAR | 单位 | `%` |
| `source_api` | VARCHAR | 数据来源接口 | `eastmoney` |
| `category` | VARCHAR | 指标分类 | `盈利能力` |
| `description` | TEXT | 备注/定义 | - |
| `updated_at` | DATETIME | 最后更新时间 | - |

## 3. 数据获取策略

### 3.1 财务指标 (fin_indicator)
**挑战**：单位被清洗脚本剥离了。
**策略**：
1. **重构字典生成器**：编写脚本 `extract_em_metadata.py`，调用 `ak.stock_financial_analysis_indicator_em`。
2. **捕获原始名**：获取 API 返回的原始列名（如 `净资产收益率(加权)(%)`）。
3. **解析**：
    - 正则提取单位：`(%)` -> `%`, `(元)` -> `元`, `(次)` -> `次`。
    - 提取纯名：作为 `column_name`（需应用与 `FinancialCollector` 完全一致的清洗逻辑）。
4. **生成**：`fin_indicator_catalog.csv`。

### 3.2 三大报表 (fin_*_statement)
**挑战**：标准会计科目，API 返回的通常是纯中文（如 `货币资金`），不带单位后缀。
**策略**：
1. **默认规则**：三大报表绝大多数科目单位为 **CNY (元)**。
2. **特殊处理**：
    - 利润表中的 `基本每股收益` 单位为 `元/股`。
    - 扫描新浪接口返回的 DataFrame，检查是否有隐藏的元数据行。
    - 若 API 无单位，则应用“会计准则默认值”策略。

## 4. 实施步骤 (Action Plan)

- [ ] **Step 1: 编写 EM 元数据提取器**
    - 创建 `workspace/extract_catalog_em.py`。
    - 逻辑：拉取东财数据 -> 解析原始列名中的单位 -> 生成 Catalog 记录。

- [ ] **Step 2: 编写 Sina 报表元数据扫描器**
    - 创建 `workspace/extract_catalog_sina.py`。
    - 逻辑：拉取三大表 -> 提取所有出现的列名 -> 标记默认单位 '元' -> 标记特殊列（如 EPS）。

- [ ] **Step 3: 创建并填充 data_catalog**
    - 创建 `storage/database/catalog_store.py`。
    - 合并 Step 1 & 2 的结果，写入 DuckDB。

- [ ] **Step 4: 集成到采集流程**
    - 修改 `FinancialCollector`，每当发现新列时，触发 Catalog 更新（可选，Phase 2 实现）。
