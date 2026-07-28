# Codex Workflows

本目录存放 Codex 在 QuantPyLab 中使用的任务流程说明。

Codex 不会自动执行 OpenCode/Claude Code 的项目内斜杠命令。用户可以直接用自然语言触发任务，例如“按标准个股研究流程分析某公司”或“按情报同步流程更新某公司”，Codex 应根据 `AGENTS.md` 的任务路由读取本目录中的对应 workflow。

## Workflow 清单

- `development.md`：开发、修复、重构、排查系统功能。
- `equity-research.md`：个股深度研究、重写研报、研报复核、深度讨论。
- `intelligence-sync.md`：个股最新公告、新闻、财报、关注点与增量情报同步。
- `report-update.md`：行业、专题或 `investigation/` 下研报的数据更新。
- `git-commit.md`：提交前审查、精确暂存、中文 commit message、提交与推送。

## 使用原则

1. workflow 是任务 SOP，不是硬规则；硬规则以 `AGENTS.md` 为准。
2. `.opencode/` 仍供 OpenCode 使用，Codex 不得为了迁移而修改它。
3. 任何需要写代码、改文档或更新研报的任务，都必须遵守项目的提案与确认机制。
4. 任何数据查询都必须通过 `storage/database/views/` 下定义的视图完成。
