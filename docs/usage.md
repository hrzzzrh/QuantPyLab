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

## 5. 环境维护
- **上下文刷新**: 在 Gemini CLI 中执行 `/memory refresh`。
- **命名准则**: 本项目严禁使用模糊命名，所有新增指令必须符合 `动作-对象` 规范。
