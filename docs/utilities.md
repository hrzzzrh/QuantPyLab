# 工具函数库 (Utilities)

本文档记录 QuantPyLab 中封装的通用工具函数，供开发者在采集、分析和回测逻辑中调用。

---

## 1. 交易日历 (`utils/trade_date.py`)

### `get_latest_trade_date`
获取最近一个已收盘的交易日日期。

- **功能**: 
    - 自动识别周末和节假日（返回上一个交易日）。
    - 自动判断收盘时间：如果今天是交易日，但在 **15:30** 之前调用，仍会返回上一个交易日，以确保数据已完全收盘可抓取。
    - **缓存机制**: 
        - **持久化缓存**: 数据保存在 `data/warehouse/metadata/trade_calendar.parquet`，每日仅在本地数据过期时请求网络接口。
        - **内存缓存**: 同一个进程生命周期内，重复调用将直接返回内存中的结果。
- **输入**: 
    - `ref_date` (datetime, 可选): 参考时间，默认为当前系统时间。
- **输出**: 
    - `date` (datetime.date): 最近交易日对象。
- **示例**:
```python
from utils.trade_date import get_latest_trade_date

# 假设今天是 2025-02-08 (周六)
ld = get_latest_trade_date()
print(ld)  # 输出: 2025-02-07
```

---

## 2. 日志工具 (`utils/logger.py`)

### `logger`
全局统一的日志对象。

- **功能**: 同时输出到控制台 (Console) 和文件 (`logs/app.log`)。
- **文件轮转**: 日志文件按天轮转 (`when="midnight"`)，当日写入 `logs/app.log`，次日自动归档为 `logs/app.log.YYYY-MM-DD`；`logs/error.log` (仅 Error) 同样按天轮转。保留天数由 `config/settings.py` 的 `LOG_RETENTION_DAYS` 控制 (默认 30 天)，超期备份自动删除。
- **测试隔离**: 设置环境变量 `QUANTPYLAB_DISABLE_FILE_LOGGING=1` 可禁用文件 handler；`setup_logger(..., enable_file_handlers=False)` 可对单个日志器显式禁用，默认仍写入文件。
- **级别**: 默认 INFO，可根据需要调整。
- **示例**:
```python
---

## 3. 财务报告期工具 (`utils/financial.py`)

### `get_previous_report_date`
推算上一个标准的季度报告期（3/31, 6/30, 9/30, 12/31）。

### `get_consecutive_reports`
获取连续的 $N$ 个报告期列表。常用于校验财务数据的完整性。
- **场景**: 用于 TTM 计算前的“数据齐全度”自检。
- **示例**:
```python
from utils.financial import get_consecutive_reports
# 获取 2025Q3 及其之前的共 5 个季度
list = get_consecutive_reports("20250930", 5)
# 输出: ['20250930', '20250630', '20250331', '20241231', '20240930']
```

---

## 4. Exa 搜索 MCP 服务器 (`tools/exa_mcp_server.py`)

网络信息检索基础设施。以本地 stdio MCP 服务器形式向 opencode 提供 Exa 搜索能力，替代了 opencode 内置的 `websearch`/`webfetch` 工具（已在 `.opencode/opencode.json` 中禁用）。

### 提供的 MCP 工具

| 工具名 | 功能 |
|---|---|
| `web_search_exa` | 常规网络搜索，返回标题/URL/发布日期/高亮摘要 |
| `web_fetch_exa` | 按 URL 抓取网页正文为纯文本（单次最多 10 个 URL） |
| `web_search_advanced_exa` | 高级搜索：品类（news/company/paper 等）、域名包含/排除、发布时间区间过滤 |

### Key 配置（多 key 轮换）

- 优先读取环境变量 `EXA_API_KEYS`（逗号分隔多个 key）。
- 兜底读取 `config/exa_keys.json`（已被 gitignore，不入库），格式：`{"keys": ["key1", "key2"]}`。
- 轮换策略：round-robin；某 key 触发 429 限流时自动切换下一 key 重试。

### 运行方式

```bash
uv run tools/exa_mcp_server.py   # 由 .opencode/opencode.json 的 local MCP 配置自动拉起
```
