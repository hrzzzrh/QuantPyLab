# 项目操作手册

本文档提供 QuantPyLab 命令行工具 `main.py` 的详细操作指南。所有指令均遵循 **“动作-对象”** 命名规范。

## 1. 基础命令结构
格式：`uv run main.py <subcommand> [options]`

### 通用参数 (Common Options)
- `--symbol CODE`: 指定单只股票。支持 6 位代码 (如 `600519`) 或带前缀代码 (如 `sh600519`)。
- `--force-all`: 强制全量扫描。忽略增量判断逻辑，对所有活跃股票执行操作。

---

## 2. 命令详解

### 2.1 全量同步 (`sync-all`)
一键执行全流程同步流水线。
- **执行顺序**: `financial` -> `indicators` -> `ttm` -> `share` -> `kline`
- **示例**: `uv run main.py sync-all`

### 2.2 基础同步命令
| 子命令 | 说明 | 增量逻辑 | 特有参数 |
| :--- | :--- | :--- | :--- |
| `sync-stocks` | 同步 A 股全量代码与名称 | 每次清空并重建 stocks 表 | 无 |
| `sync-metadata` | 同步行业、上市日期等元数据 | 自动识别缺失字段补全 | `--industry`, `--list-info` |
| `sync-financial` | 同步财务三报表原始数据 | 披露日历驱动 + 孤儿股补全 | 无 |
| `sync-indicators`| 同步东财计算指标 | 披露日历驱动 + 孤儿股补全 | 无 |
| `calc-ttm` | 计算 TTM 滚动财务数据 | **差异驱动**: 校验最近 5 季数据齐全后补算 | 无 |
| `sync-share` | 同步股本变动记录 | **自动续传**: 从本地最大日期+1同步 | `--start-date` |
| `sync-kline` | 同步日线行情 | **自动续传**: 从本地最大日期+1同步 | `--start-date` |

### 2.3 开发工具命令
| 子命令 | 说明 | 参数 |
| :--- | :--- | :--- |
| `export-views` | 导出 DuckDB 视图 SQL 脚本 | `[--output]` (默认: `docs/view_definition.sql`) |
| `show-views` | 显示视图依赖拓扑图 | 无 |

---

## 3. 数据查询示例

### 3.1 Python 接入
```python
from storage.database.manager import db_manager
conn = db_manager.get_duckdb_conn() # 自动加载所有视图
df = conn.execute("SELECT * FROM v_daily_valuation WHERE symbol = '600519' LIMIT 10").df()
```

### 3.2 外部工具 (DBeaver)
1. 运行 `uv run main.py export-views`。
2. 在 DBeaver (DuckDB) 中执行 `docs/view_definition.sql`（Database 建议设为 `:memory:`）。
3. 即可直接查询逻辑视图。

---

## 4. 环境维护
- **上下文刷新**: 在 Gemini CLI 中执行 `/memory refresh`。
- **命名准则**: 本项目严禁使用模糊命名，所有新增指令必须符合 `动作-对象` 规范。
