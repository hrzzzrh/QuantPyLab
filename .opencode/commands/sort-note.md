---
description: 规范并整理 note.md 中两个表格的排序
---
请按 `AGENTS.md` 的「跟踪审计日历」规则处理 `investigation/equities/note.md`，随后执行 `PYTHONPATH=. uv run python workspace/scripts/sort_note_tables.py`。脚本会校验日期格式，再整理两个主表格；若校验失败，修正被报告的行后重新执行。最后报告结果。
