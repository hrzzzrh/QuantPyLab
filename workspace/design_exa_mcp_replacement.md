# 设计提案：以 Exa MCP 替换内置 websearch/webfetch 并实现多 Key 轮换

## 1. 背景与目标

项目当前的信息检索依赖 opencode 内置工具 `websearch` / `webfetch`（被 `deep-researcher` skill 与 `updateIndustryReport` 命令引用）。用户希望改用 Exa AI 搜索服务，要求：

1. **完全替代**：禁用内置 `websearch` / `webfetch`，所有搜索统一走 Exa。
2. **多 API Key 轮换**：用户持有多个 Exa API key，要求实现轮换使用（突破单 key 限流）。
3. **工具范围**：搜索（`web_search_exa`）、网页抓取（`web_fetch_exa`）、高级搜索（`web_search_advanced_exa`）。

## 2. 方案选型

| 方案 | 说明 | 结论 |
|---|---|---|
| A. 远程 MCP 直连 `https://mcp.exa.ai/mcp` | 配置最简单，官方推荐 | 仅支持单个静态 `x-api-key` header，**无法轮换**，弃 |
| B. 本地 HTTP 反代 + 远程 MCP | 自建代理轮换 key 后转发 | 需常驻进程、端口管理、会话转发，复杂 |
| C. **本地 stdio MCP 服务器（Python）** | 用 `mcp` (FastMCP) 包实现，直接调用 Exa REST API，key 轮换内置于服务器 | **采纳**：与项目 uv/Python 体系一致，无 npm 依赖，可测试 |

选择 C 的理由：
- Exa 提供公开 REST API（`POST https://api.exa.ai/search`、`POST https://api.exa.ai/contents`），轮换逻辑（round-robin + 429 退避）内聚在服务器内。
- opencode 原生支持 local MCP（stdio 传输），`command` 指向 `uv run` 即可，无端口/常驻进程。
- 工具命名沿用 Exa MCP 官方命名（`web_search_exa` 等），skills/commands 中的引用改动最小。

## 3. 架构

```
opencode (agent)
   │  stdio (MCP JSON-RPC)
   ▼
exa_mcp_server (Python, FastMCP, 本地进程, uv run)
   │  key 轮换 (round-robin + 429 重试)
   ▼
Exa REST API (search / contents)
```

## 4. 实现细节

### 4.1 新增依赖

`pyproject.toml` 添加 `mcp>=1.0`（FastMCP 框架，stdio 默认传输）。

### 4.2 服务器模块（新增 `tools/exa_mcp_server.py`）

- `ExaClient`：封装 REST 调用，持有 key 列表；`_next_key()` 做轮换；HTTP 429 时切换下一 key 重试一次。
- 三个 MCP 工具（FastMCP 注册）：

| 工具名 | 参数 | 对应 Exa API |
|---|---|---|
| `web_search_exa` | `query`, `num_results=8` | `/search`（auto 类型，返回 title/url/高亮摘要） |
| `web_fetch_exa` | `urls[]` | `/contents`（text 转 markdown 简洁模式） |
| `web_search_advanced_exa` | `query`, `num_results`, `category`, `include_domains`, `exclude_domains`, `start_published_date`, `end_published_date`, `contents_type` | `/search` 全参数面 |

- 密钥来源：环境变量 `EXA_API_KEYS`（逗号分隔，优先级最高），其次 `config/exa_keys.json`（gitignore，便于本机存放多 key）。`config/exa_keys.json` 不存在时给出清晰错误提示。

### 4.3 opencode 配置（`.opencode/opencode.json`）

```json
{
  "mcp": {
    "exa": {
      "type": "local",
      "command": ["uv", "run", "tools/exa_mcp_server.py"],
      "environment": { "EXA_API_KEYS": "{env:EXA_API_KEYS}" }
    }
  },
  "tools": {
    "websearch": false,
    "webfetch": false
  },
  "permission": { "websearch": "deny", "webfetch": "deny" }
}
```

说明：`environment` 支持 `{env:...}` 引用宿主环境变量，key 不落盘于配置文件。

### 4.4 文档与 skill 同步

- `.opencode/commands/investmentAnalysis/updateIndustryReport.md`：`websearch` 引用改为 `web_search_exa` / `web_search_advanced_exa`。
- `.opencode/skills/deep-researcher/SKILL.md`：在"动作 (Act)"一节补充 Exa 工具使用指引（并发搜索用 `web_search_exa`，全文抓取用 `web_fetch_exa`，限定域/日期用 `web_search_advanced_exa`）。
- `docs/` 已核查无 websearch 引用，无需改动。

## 5. 验证方案

1. 单元级：`workspace/` 下写测试脚本，直接调用 `ExaClient`，用真实 key 验证搜索/抓取/高级搜索三条路径，并验证轮换（打印 key 后缀）。
2. 协议级：用管道向服务器发送 JSON-RPC（initialize → tools/list → tools/call），验证 MCP 握手与工具注册。
3. 集成级：用户重启 opencode 会话后，工具应出现在会话中，`updateIndustryReport` / `deep-researcher` 流程可正常检索。

## 6. 影响面与风险

- **影响面**：`pyproject.toml`、`.opencode/opencode.json`、两个 md 文件、新增 `tools/exa_mcp_server.py`。用户明确点名要求修改 `.opencode/`，符合 AGENTS.md 约束。
- **风险**：Exa REST API 参数与 MCP 官方实现细节可能有出入 → 以验证步骤 1 实测为准（数据怀疑论原则）。
- **回退**：删除 mcp 配置、恢复 `tools` 与 `permission` 即回退；`git` 可整体回滚。

## 7. 待确认

本提案在对话中获得用户明确"确认锁"后开始开发。
