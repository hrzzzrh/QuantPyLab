# Development Workflow

本 workflow 用于开发、修复、重构、排查系统功能。

## 入口判断

当用户要求修改代码、实现功能、修复 bug、调整 CLI、更新数据视图、改存储逻辑或排查系统行为时，使用本 workflow。

## 执行流程

1. 先阅读 `docs/` 中与任务相关的文档。常用入口包括：
   - `docs/architecture.md`
   - `docs/usage.md`
   - `docs/storage_architecture_rfc.md`
   - `docs/view_architecture.md`
   - `docs/data_schema.md`
   - `docs/data_catalog.md`
   - `docs/utilities.md`
2. 判断任务复杂度。
   - 简单变更：先在对话中提出简短方案，获得用户明确确认后再改文件。
   - 复杂变更：先在 `workspace/` 编写设计 proposal，获得用户明确确认锁后再改文件。
3. 修改代码时保持局部、精确、符合现有结构。
4. 所有脚本实验必须写到 `workspace/` 下，再用 `uv run` 执行。
5. 数据查询必须通过视图完成。新增或修改查询能力时，优先扩展 `storage/database/views/` 中的 Python 视图类。
6. 涉及图表或架构图时，使用 PlantUML。
7. 验证应与风险匹配。优先运行最小相关命令，例如：

```bash
uv run main.py show-views
uv run main.py <subcommand> [options]
```

## 禁止事项

- 禁止全量或递归扫描 `data/` 目录。
- 禁止直接在命令行用 `uv run` 包裹临时 Python 代码。
- 禁止用 `git add .`。
- 禁止在未确认的情况下执行复杂开发改动。
