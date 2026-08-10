# 项目操作手册

本文档提供 QuantPyLab 命令行工具 `main.py` 的详细操作指南。所有指令均遵循 **“动作-对象”** 命名规范。

## 1. 基础命令结构
格式：`uv run main.py <subcommand> [options]`

### 通用参数 (Common Options)
- `--symbol CODE`: 指定单只股票。支持 6 位纯数字代码 (如 `600519`)。
  - **单股模式**: 绕过披露日历等全局筛选逻辑，强制对该 symbol 执行同步流程。
- `--force-all`: 强制全量扫描。忽略增量判断逻辑，对所有股票执行操作。

---

## 2. 增量同步策略

本项目根据数据特性采取不同的增量同步策略，以平衡数据一致性与传输效率：

| 数据类别 | 抓取策略 | 存储策略 | 逻辑说明 |
| :--- | :--- | :--- | :--- |
| **财务报表/指标** | **单股全量抓取** | **增量合并 (Upsert)** | 抓取该股所有历史年度数据；存储时按 `report_date` 合并去重。 |
| **TTM 计算** | **全量重算** | **全量覆盖** | 基于该股所有历史报表重新计算所有报告期的 TTM 并刷新本地分片。 |
| **股本变动** | **本地增量过滤** | **增量合并 (Upsert)** | **全量抓取+本地过滤**: 抓取新浪全量历史，仅保存本地最新 `change_date` 之后的数据。另通过 `metadata.db` 的 `sync_status` 表记录每股最后同步成功日期，当日已同步的股票在批量模式下自动跳过 (重跑只补失败的)。 |
| **日线 K 线** | **真增量抓取** | **增量合并 (Upsert)** | **自动续传**: 仅抓取本地最新 `date` 之后的数据。退市股改用腾讯源全量重建 (见 3.2 表 `sync-kline` 行)。 |

## 3. 命令详解

### 3.1 全量同步 (`sync-all`)
一键执行全流程同步流水线。
- **执行顺序**: `stocks` (名单 diff + 退市清单合并) -> `metadata` (行业/地域/上市日期) -> `indicators` -> `financial` -> `ttm` -> `share` -> `kline`
- **示例**: `uv run main.py sync-all`
- **单股示例**: `uv run main.py sync-all --symbol 600519` (强制刷新该股所有财务数据并增量补全行情; 名单与元数据为全市场操作, 单股模式跳过)

### 3.2 基础同步命令
| 子命令 | 说明 | 增量逻辑 | 特有参数 |
| :--- | :--- | :--- | :--- |
| `sync-stocks` | 同步 A 股全量代码与名称 | **差量 diff**: 新增插入、存量更新名称、消失标记退市 (is_active=0)；随后**合并沪深退市股清单** (补齐历史退市股, 重建场景必需)；last_trade_date 由退市股 K 线重建流程写入 | 无 |
| `sync-metadata` | 同步行业、上市日期等元数据 | 自动识别缺失字段补全；行业由雪球个股资料补全 (东财 push2 接口已风控弃用)，地域/上市日期由雪球→东财→巨潮三级兜底 | `--industry`, `--list-info` |
| `sync-financial` | 同步财务三报表原始数据 | 披露日历驱动 + 孤儿股补全 | 无 |
| `sync-indicators`| 同步东财计算指标 | 披露日历驱动 + 孤儿股补全 | 无 |
| `calc-ttm` | 计算 TTM 滚动财务数据 | **差异驱动**: 校验最近 5 季数据齐全后补算。候选集以数据湖实际存在的报表为准 (含孤儿股/退市股)，不依赖 stocks 表 | 无 |
| `sync-share` | 同步股本变动 (新浪源) | **本地增量**: 从本地最大日期后补全；默认批量模式跳过当日已同步股票 (见 `sync_status` 表)，`--symbol`/`--force-all` 强制绕过 | `--start-date` |
| `sync-kline` | 同步日线行情 | **自动续传**: 从本地最大日期+1同步；**退市股 (is_active=0) 改用腾讯源全量重建** (单一复权口径, 完成后写 sync_status 跳过) | `--start-date` |
| `sync-etf-list` | 同步场内交易基金列表 | 每次清空并重建 etfs 表 | 无 |
| `sync-etf-kline` | 同步ETF日线行情 | **自动续传**: 从本地最大日期后补全 | `--start-date` |

### 2.3 开发工具命令
| 子命令 | 说明 | 参数 |
| :--- | :--- | :--- |
| `export-views` | 导出 DuckDB 视图 SQL 脚本 | `[--output]` (默认: `docs/view_definition.sql`) |
| `show-views` | 显示视图依赖拓扑图 | 无 |
| `rebuild-schemas` | 重建视图 schema 预声明缓存 | `[--dataset]` (默认: 全部) |
| `run-backtest` | 按 TOML 运行日频股票策略回测 | `--backtest-config PATH` |
| `list-backtest-strategies` | 列出已注册的日频回测策略 | 无 |

> **何时需要 `rebuild-schemas`**：视图采用 schema 预声明机制（见 4.4 节），schema 缓存为静态快照。当财务字段新增/变更（东财新增指标列、报表科目调整）或同步后出现 schema 相关错误时，必须执行 `uv run main.py rebuild-schemas` 重建缓存，否则新列查询会静默返回 NULL。

### 2.4 代码质量检查 (Lint & Format)

本项目使用 `ruff` 统一管理 lint 与代码格式（配置见 `pyproject.toml` 的 `[tool.ruff]`）。检查范围为核心代码（`storage/`、`backtest/`、`data_ingestion/`、`utils/`、`config/`、`analysis/`、`tools/`、`main.py`、`tests/`），`workspace/` 下的开发脚本被排除。

```bash
uv run ruff check .        # 静态检查（规则: E/F/I/UP）
uv run ruff format .       # 自动格式化
uv run ruff format --check .   # 检查格式是否符合规范（CI 用）
```

> **E501 行长度说明**：`E501` 已配置忽略。结构性超长行由 `ruff format` 自动换行；剩余的 SQL/HTML/中文标签等字符串字面量不做强制拆行。

### 3.3 日频回测 (`run-backtest`)

`run-backtest` 读取 TOML 配置并执行已注册策略。当前内置 `quality-value-recovery`（低估值、质量与趋势）和 `price-momentum`（中期动量与趋势）两种策略。所有策略均在每月最后一个交易日收盘后产生信号，并在下一交易日开盘成交；估值使用不复权价格，收益使用后复权价格。

```bash
uv run main.py list-backtest-strategies

uv run main.py run-backtest \
  --backtest-config config/backtest/quality_value_recovery.toml
```

示例 TOML 位于 `config/backtest/`。将 `[run]` 中的 `benchmark_symbol` 设为空字符串可跳过 ETF 基准。每次运行会在 `workspace/backtest/results/` 创建独立目录，保存解析后的参数 JSON、每日净值、调仓目标、成交记录和摘要；该目录是实验产物，不纳入版本控制。完整的策略参数、数据口径与扩展边界见 [日频股票回测](backtest.md)。

### 3.4 定时调度 (每日凌晨 03:00 sync-all)

基于 **launchd LaunchAgent** 实现每日自动同步，入口脚本为 `tools/schedule_sync_all.py`，调度配置模板为 `config/launchd/com.quantpylab.sync-all.plist`（含机器绝对路径，换机/重建 venv 需同步修改）。

**触发与判定逻辑**（每日 03:00 触发一次 wrapper）：
1. 项目根目录不存在（外置卷未挂载）→ 记日志并以失败退出
2. 安装新浪源请求保护层（幂等，覆盖交易日历请求）
3. 前一天 (today-1) 非交易日 → 退出（零同步请求）
4. 前一天是交易日 且 已记录 sync-all 成功（`last_sync_date >= 前一天`）→ 退出
5. 执行 sync-all 流水线；**未全部成功时整体重试**（增量机制自愈：重跑只补失败部分）
6. 全部成功 → 记录状态 `sync_status` 表 (`dataset='sync_all', symbol='ALL'`, **日期=前一天数据日**)，保证每个交易日数据在次日凌晨入库、无延迟；失败/中止 → 不记录，次日自动补跑

**成功判定（三态）**：`sync_all_data_flow` 汇总 7 个环节（stocks/metadata/indicators/financial/ttm/share/kline）的失败计数：
- `success`：全部环节失败数为 0
- `retryable`：任一环节存在失败 → 整体重试（次数 `SYNC_ALL_MAX_RETRIES`，间隔 `SYNC_ALL_RETRY_INTERVAL_SECONDS`，见 `config/settings.py`）
- `blocked`：新浪 IP 风控（含 kline/share 环节传播）→ 不重试（等待解封），次日补跑

**CLI 退出码**：手动执行 `uv run main.py sync-all` 时，`blocked` 和 `retryable` 状态均退出码为 1；全部成功才退出码为 0。`retryable` 可重跑，增量逻辑会自动补缺。

**安装**：
```bash
cp config/launchd/com.quantpylab.sync-all.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.quantpylab.sync-all.plist
```

**卸载**：
```bash
launchctl unload ~/Library/LaunchAgents/com.quantpylab.sync-all.plist
rm ~/Library/LaunchAgents/com.quantpylab.sync-all.plist
```

**查看运行状态与日志**：
```bash
launchctl list | grep quantpylab
tail -f logs/error.log                   # 错误日志
tail -f logs/app.log                     # 流水线日志
sqlite3 data/metadata.db "SELECT * FROM sync_status WHERE dataset='sync_all'"
```

**手动试跑**（安全：非交易日或已成功会直接退出）：
```bash
uv run python tools/schedule_sync_all.py
```
（`tools/` 为项目核心脚本目录，此写法与 `uv run main.py` 同类，不违反"禁止命令行 uv run 包裹临时代码"规范；或直接执行 `/Volumes/wdblack/some_project/QuantPyLab/.venv/bin/python tools/schedule_sync_all.py`，与 launchd 调用方式一致。）

> **注意**：launchd 的 stdout/stderr 管道为始终存在的 `/dev/null`；本地 `/bin/sh` 只负责确认项目外置卷和 `logs/` 可用，再以绝对路径启动 Python。卷未挂载时记录到 `~/Library/Logs/QuantPyLab/launcher.log` 并失败退出；正常运行的流水线日志由项目日志器写入 `logs/app.log` / `logs/error.log`。

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
在编写 `workspace/research/` 下的分析脚本时，**必须使用 `db_manager`**，并在查询前通过 `ensure_views(...)` 显式声明所需视图（DAG 依赖自动带出）。研究产生的结论性文档应存放在 `investigation/` 对应子目录下。

**最小查询模板 (`investigate_data.py`)：**
```python
from storage.database.manager import db_manager
import pandas as pd


def main():
    # 1. 获取连接 (视图默认不加载，需显式声明)
    conn = db_manager.get_duckdb_conn()
    db_manager.ensure_views("v_daily_valuation")  # 依赖视图 (daily_kline 等) 自动注册
    symbol = "002487"

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
        db_manager.close_all()  # 务必关闭连接释放资源
```

### 4.3 路径 C：外部工具 (DBeaver)
1. 运行 `uv run main.py export-views` 生成 SQL 定义。
2. 在 DBeaver 中新建 DuckDB 连接。
3. 执行导出的 SQL 脚本即可直接在可视化界面查询逻辑视图。

### 4.4 视图加载机制：按需注册 + schema 预声明
视图系统采用两层机制，保证内存占用可控（初始化峰值 < 0.2GB，全量查询峰值 < 1.5GB）：

1. **按需注册 (Lazy Loading)**：`get_duckdb_conn()` 不自动注册任何视图；通过 `db_manager.ensure_views('view_name', ...)` 声明所需视图，系统按 DAG 拓扑序注册（含全部依赖），已注册视图自动跳过。
2. **schema 预声明 (Schema Predeclaration)**：视图 SQL 通过 `read_parquet(..., schema=MAP(...))` 预声明列集与类型，替代 `union_by_name=1` 的运行时全分片 schema 推断（后者需扫描全部 5500+ 分片 footer，导致 6-7GB 峰值内存）。schema 缓存位于 `storage/database/views/schemas/<dataset>.json`，由 `rebuild-schemas` 命令生成。
   - `symbol` 分区列通过 `filename=true` + `regexp_extract(filename, 'symbol=(\d+)', 1)` 从路径提取。
   - **schema 外列查询静默返回 NULL 而非报错**：字段变更后必须执行 `uv run main.py rebuild-schemas`，否则新列数据不可见。

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
query = """
    SELECT report_date, "营业收入"/1e8 as rev_100m, "归属于母公司所有者的净利润"/1e8 as np_100m
    FROM fin_income_statement WHERE symbol='002594'
"""
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
