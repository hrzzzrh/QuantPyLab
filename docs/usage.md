# 项目操作手册

本文档提供 QuantPyLab 命令行工具 `main.py` 的详细操作指南。所有指令均遵循 **“动作-对象”** 命名规范。

## 1. 基础命令结构
格式：`uv run main.py <subcommand> [options]`

### 通用参数 (Common Options)
- `--symbol CODE`: 指定单只股票。支持 6 位纯数字代码 (如 `600519`)。
  - **单股模式**: 绕过披露日历等全局筛选逻辑，强制对该 symbol 执行同步流程。
- `--force-all`: 强制全量扫描。忽略增量判断逻辑，对所有活跃股票执行操作。

---

## 2. 增量同步策略

本项目根据数据特性采取不同的增量同步策略，以平衡数据一致性与传输效率：

| 数据类别 | 抓取策略 | 存储策略 | 逻辑说明 |
| :--- | :--- | :--- | :--- |
| **财务报表/指标** | **单股全量抓取** | **增量合并 (Upsert)** | 抓取该股所有历史年度数据；存储时按 `report_date` 合并去重。 |
| **TTM 计算** | **全量重算** | **全量覆盖** | 基于该股所有历史报表重新计算所有报告期的 TTM 并刷新本地分片。 |
| **股本变动** | **本地增量过滤** | **增量合并 (Upsert)** | **全量抓取+本地过滤**: 抓取新浪全量历史，仅保存本地最新 `change_date` 之后的数据。 |
| **日线 K 线** | **真增量抓取** | **增量合并 (Upsert)** | **自动续传**: 仅抓取本地最新 `date` 之后的数据。 |

## 3. 命令详解

### 3.1 全量同步 (`sync-all`)
一键执行全流程同步流水线。
- **执行顺序**: `indicators` -> `financial` -> `ttm` -> `share` -> `kline`
- **示例**: `uv run main.py sync-all`
- **单股示例**: `uv run main.py sync-all --symbol 600519` (强制刷新该股所有财务数据并增量补全行情)

### 3.2 基础同步命令
| 子命令 | 说明 | 增量逻辑 | 特有参数 |
| :--- | :--- | :--- | :--- |
| `sync-stocks` | 同步 A 股全量代码与名称 | 每次清空并重建 stocks 表 | 无 |
| `sync-metadata` | 同步行业、上市日期等元数据 | 自动识别缺失字段补全 | `--industry`, `--list-info` |
| `sync-financial` | 同步财务三报表原始数据 | 披露日历驱动 + 孤儿股补全 | 无 |
| `sync-indicators`| 同步东财计算指标 | 披露日历驱动 + 孤儿股补全 | 无 |
| `calc-ttm` | 计算 TTM 滚动财务数据 | **差异驱动**: 校验最近 5 季数据齐全后补算 | 无 |
| `sync-share` | 同步股本变动 (新浪源) | **本地增量**: 从本地最大日期后补全 | `--start-date` |
| `sync-kline` | 同步日线行情 | **自动续传**: 从本地最大日期+1同步 | `--start-date` |
| `sync-etf-list` | 同步场内交易基金列表 | 每次清空并重建 etfs 表 | 无 |
| `sync-etf-kline` | 同步ETF日线行情 | **自动续传**: 从本地最大日期后补全 | `--start-date` |

### 2.3 开发工具命令
| 子命令 | 说明 | 参数 |
| :--- | :--- | :--- |
| `export-views` | 导出 DuckDB 视图 SQL 脚本 | `[--output]` (默认: `docs/view_definition.sql`) |
| `show-views` | 显示视图依赖拓扑图 | 无 |

---

## 4. 数据查询指南 (Data Query Guide)

本项目的所有数据均通过 **DuckDB 逻辑视图 (Unified View)** 暴露。你可以通过以下两种标准路径进行查询：

### 4.1 路径 A：交互式 CLI 查询 (零代码)
适用于快速验证单只股票的状态或结构，推荐直接使用 `duckdb` 命令行工具。

**示例：查询某股票的 TTM 财务指标**
```bash
# 直接在命令行执行（需系统安装 duckdb）
duckdb -c "
INSTALL icu; LOAD icu;
SELECT report_date, revenue_ttm/1e8 as rev_100m, net_profit_ttm/1e8 as np_100m 
FROM read_parquet('data/warehouse/financial/ttm/symbol=002487/*.parquet') 
ORDER BY report_date DESC LIMIT 5;"
```

**示例：查询估值视图 (需加载视图定义)**
```bash
# 先导出视图定义，然后交互查询
uv run main.py export-views -o /tmp/views.sql
duckdb -init /tmp/views.sql -c "SELECT * FROM v_daily_valuation WHERE symbol='600519' LIMIT 5"
```

### 4.2 路径 B：编程式查询模板 (推荐用于分析)
在编写 `workspace/research/` 下的分析脚本时，**必须使用 `db_manager`** 以确保所有视图（DAG 依赖）被自动正确加载。研究产生的结论性文档应存放在 `investigation/` 对应子目录下。

**最小查询模板 (`investigate_data.py`)：**
```python
from storage.database.manager import db_manager
import pandas as pd

def main():
    # 1. 获取连接 (系统会自动执行所有 View 定义)
    conn = db_manager.get_duckdb_conn()
    symbol = '002487'

    # 2. 编写 SQL (推荐使用 TTM 或 估值视图)
    query = f"""
    SELECT date, market_cap/1e8 as mkt_cap, pe_ttm, pb 
    FROM v_daily_valuation 
    WHERE symbol = '{symbol}' 
    ORDER BY date DESC LIMIT 20
    """

    # 3. 转换为 DataFrame 并分析
    df = conn.execute(query).df()
    print(df)

if __name__ == "__main__":
    try:
        main()
    finally:
        db_manager.close_all() # 务必关闭连接释放资源
```

### 4.3 路径 C：外部工具 (DBeaver)
1. 运行 `uv run main.py export-views` 生成 SQL 定义。
2. 在 DBeaver 中新建 DuckDB 连接。
3. 执行导出的 SQL 脚本即可直接在可视化界面查询逻辑视图。

---

## 5. 常见陷阱 (Pitfalls)

编写分析脚本时，注意避开以下已知的易错点：

### 5.1 `adj_factor` 不在 `v_daily_valuation` 中
`v_daily_valuation` 内部使用了 `adj_factor` 计算 `close_hfq`，但**视图不暴露原始 `adj_factor` 字段**。
```python
# ❌ 错误：v_daily_valuation 没有 adj_factor 列
SELECT date, raw_close, adj_factor FROM v_daily_valuation WHERE ...

# ✅ 正确：需要复权因子时直接查基表 daily_kline
SELECT date, close, adj_factor, close * adj_factor as close_hfq
FROM daily_kline WHERE symbol = '002594'

# ✅ 正确：只需要后复权价时直接用 v_daily_valuation 的 close_hfq
SELECT date, raw_close, close_hfq FROM v_daily_valuation WHERE ...
```

### 5.2 股价类型必须显式标注
`v_daily_valuation` 包含两种收盘价，字段名已明确区分，**禁止混用**：
- `raw_close`：**不复权市价**（用于目标价定价、盈亏比计算、市值计算）
- `close_hfq`：**后复权价**（用于长周期趋势分析、均线系统、支撑阻力位判定）

```python
# ❌ 错误：把 raw_close 用于 MA 趋势分析（受分红除权干扰）
SELECT AVG(raw_close) FROM ...

# ✅ 正确：用后复权价做技术分析
df['ma60'] = df['close_hfq'].rolling(60).mean()
```

### 5.3 财务表列名为中文，SQL 中需正确转义
三大报表 (`fin_balance_sheet`, `fin_income_statement`, `fin_cashflow_statement`) 和指标表 (`fin_indicator`) 的列名为中文，在 Python 字符串中需要双引号包裹：
```python
# ✅ 正确：双引号（SQL 标识符）包裹中文列名
query = '''
    SELECT report_date, "营业收入"/1e8 as rev_100m, "归属于母公司所有者的净利润"/1e8 as np_100m
    FROM fin_income_statement WHERE symbol='002594'
'''
```

### 5.4 `report_date` 类型为 VARCHAR 非 DATE
财务报表的 `report_date` 存储为 `VARCHAR`（如 `'20251231'`），不能用 `>=` 直接与 Python 的 `datetime.date` 比较：
```python
# ✅ 正确：字符串比较
WHERE report_date >= '20200101'

# ❌ 错误：date 类型与 varchar 混用
WHERE report_date >= '2020-01-01'  # 格式不匹配
```

### 5.5 Parquet 分区路径格式
若需直接读 Parquet（而非通过视图），Hive 分区的路径格式必须精确匹配，不可猜测：
```
# 正确路径格式（参考 docs/view_definition.sql）
data/warehouse/daily_kline/*/*.parquet
data/warehouse/financial/ttm/*/*.parquet
data/warehouse/financial_statements/type=balance/*/*.parquet
data/warehouse/indicators/*/*.parquet
data/warehouse/share_capital/*/*.parquet
```

### 5.6 `report_date` 格式在 TTM 表中为 `YYYYMMDD`
`fin_ttm.report_date` 和 `pub_date` 均为 `YYYYMMDD` 格式的字符串（如 `'20260331'`），在用 `ORDER BY report_date` 之前无需转换，字符串排序即等于日期排序。但在需要与 `v_daily_valuation.date`（DATE 类型）进行 JOIN 时，需使用 `strptime(pub_date, '%Y%m%d')::DATE` 进行类型转换。

### 5.7 务必 `close_all()` 释放资源
使用 `db_manager` 的脚本必须在最后调用 `close_all()`：
```python
if __name__ == "__main__":
    try:
        main()
    finally:
        db_manager.close_all()  # 必备，防止资源泄漏
```

### 5.8 `uv run python script.py` 需加 `PYTHONPATH=.`

Python 运行脚本文件时会将**脚本所在目录**（而非当前工作目录）加入 `sys.path[0]`。因此从 `workspace/research/` 下的脚本 `import storage.database.manager` 会失败：

```bash
# ❌ 错误：ModuleNotFoundError: No module named 'storage'
cd /path/to/project
uv run python workspace/research/某公司/scripts/query.py

# ✅ 正确：显式指定 PYTHONPATH 为项目根目录
cd /path/to/project
PYTHONPATH=. uv run python workspace/research/某公司/scripts/query.py
```

> **原因**：`uv run` 保持了 Python 的标准行为——执行文件时 `sys.path[0]` 指向文件所在目录。只有执行 `-c` 内联代码或 `-m` 模块时，`sys.path[0]` 才为当前工作目录。因此 `uv run python -c "from storage.database.manager import db_manager"` 能正常工作，但 `uv run python path/to/script.py` 不能。

### 5.9 必须用 `uv run python`，不能用系统 `python3`

项目的所有依赖（`duckdb`、`pandas` 等）安装在 `.venv` 中，系统 `python3` 无法访问：

```bash
# ❌ 错误：ModuleNotFoundError: No module named 'duckdb'
python3 -c "from storage.database.manager import db_manager"

# ✅ 正确：通过 uv 运行以激活虚拟环境
uv run python -c "from storage.database.manager import db_manager"
```

### 5.10 `v_daily_valuation` 不含 OHLCV 行情字段

`v_daily_valuation` 是估值分析视图，仅暴露计算估值所需的字段（`raw_close`、`close_hfq`、`total_shares`、`market_cap`、`pe_ttm`、`pb`、`ps_ttm`、`pcf_ttm`）。**不含 `open`、`high`、`low`、`volume`、`amount`**。

做技术面分析（成交量、波动率、均线系统等）时需要 JOIN `daily_kline`：

```python
# ❌ 错误：v_daily_valuation 没有 open/volume 列
SELECT date, open, high, low, close, volume
FROM v_daily_valuation WHERE symbol = '002028'

# ✅ 正确：JOIN daily_kline 获取完整行情
SELECT k.date, k.open, k.high, k.low, k.close, k.volume, k.amount,
       v.close_hfq, v.pe_ttm
FROM daily_kline k
JOIN v_daily_valuation v ON k.symbol = v.symbol AND k.date = v.date
WHERE k.symbol = '002028'
```

> **注意**：`daily_kline` 只有不复权价格（`open/high/low/close`）和 `adj_factor`，需要后复权价格时通过 `v_daily_valuation.close_hfq` 或自行计算 `close * adj_factor` 获取。

---

## 6. 环境维护
- **上下文刷新**: 在 Gemini CLI 中执行 `/memory refresh`。
- **命名准则**: 本项目严禁使用模糊命名，所有新增指令必须符合 `动作-对象` 规范。
