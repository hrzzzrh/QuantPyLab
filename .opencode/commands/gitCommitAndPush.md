---
description: 提交并推送修改
---
请你查看除 `workspace/` 目录以外的所有变更，然后提交并推送到远端。如果变更文件较多，请仔细分析并合理拆分 commit。

执行顺序必须严格遵守：

1. 检查 `git status`、`git diff`、`git log --oneline -10`、当前分支和远端跟踪状态；不得修改或暂存 `workspace/` 变更。
2. 将全部非 `workspace/` 变更交给 `code-reviewer` 子 agent 做全维度审查，并实测 `uv run ruff check .`、`uv run ruff format --check .`、`uv run pytest tests/`。
3. 对每条审查意见修复并补测试，或记录明确的不采纳理由；修复项和不采纳项必须再次交给同一 `code-reviewer` 讨论，直到达成一致。
4. 仅在 Review Gate、Lint Gate、Test Coverage Gate 全部通过后，列出拟拆分的 commit、文件范围和命令，并停止等待用户明确确认；未确认前不得执行 `git add`、`git commit` 或 `git push`。
5. 获得确认后，按逻辑拆分并精确暂存非 `workspace/` 文件；使用中文 commit message 执行提交，检查 commit 内容后再推送到当前远端分支。
