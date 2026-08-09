# QuantPyLab 项目指南 (系统指令)
## 0.必须严格遵守开发核心原则
必须严格遵守开发核心原则
## 1. 项目定位
`QuantPyLab` 是一个基于 Python 的量化交易实验室，旨在实现从数据获取、存储、分析到回测的全流程闭环。

## 2. 文档管理体系
- **docs/**：反映系统当前现状（架构、规范、协议、数据资产）。不记录开发历史。需要查询数据，写脚本时需要先查看此目录下的文档。
    - **workflows/**：Codex 任务流程说明。用于替代 OpenCode/Claude Code 斜杠命令在 Codex 中的执行入口，描述某类任务应该如何推进。
- **investigation/**：投研产出中心。存放高质量、结论性的研究文档。
    - **equities/**：个股研究（包含跟踪手册、深度报告、投资框架）。其中 `investigation/equities/note.md` 是个股投研汇总信息文件，投研资产发生实质更新时必须同步维护。
    - **industry/**：行业研究（包含行业格局、供需分析、竞争链条）。
    - **macro/**：宏观研究（包含宏观经济指标、政策变动分析）。
- **workspace/**：存放开发中的设计稿、调研草案及工程实验室（脚本、原始财报、临时数据）。各类脚本必须写到workspace下后用uv run运行，禁止直接在命令行用uv run包裹python代码运行。
- **.opencode/**：OpenCode 专用配置、命令与 skill。该目录仍在使用，Codex 不得迁移、删除、重写或整理其中内容，除非用户明确点名要求修改 `.opencode/`。
- **绘图规范**：所有图表必须使用 **PlantUML** 语法。

## 3. 开发核心原则
1. **文档先行 (Docs-First Discovery)**：禁止直接搜索代码，必须首先阅读 `docs/`。
2. **提案与确认机制 (Proposal & Confirmation Protocol)**：本机制仅适用于功能开发工作。严禁抢跑：复杂功能变更必须先在 `workspace/` 编写设计文档并获得"确认锁"，简单功能变更可以先在对话中提出提案并获得"确认锁"，必须在用户明确回复同意执行后才能开始实际开发。研报、跟踪手册、投研汇总及其他 `investigation/` 相关工作不需要确认锁；用户提出明确任务后，可按对应 workflow 直接开展与写入。
3. **数据怀疑论 (Data Skepticism)**：绝不假设两个数据源一致，必须实测采样。
4. **信源分级 (Source Hierarchy)**：事件类事实核验优先使用公司公告、交易所、巨潮资讯、公司官网、监管机构等可追溯来源；新闻稿和媒体报道只能作为辅助证据（线索），关键数据（订单金额、业绩数字、市占率、政策表述等）必须回到公告原文或权威来源核实后才能用于结论；对关键数据至少尝试两个独立来源交叉验证；对尚未发布的新数据如实说明、不得编造或外推；对无法核实到权威来源的外部数据按可信度采信或保守修正，并注明其来源与核实状态（如"来源：XXX，未经权威来源交叉验证"）。
5. **命名确定性 (Naming Precision)**：
    - **严禁模糊**：所有函数、类、变量及命令行指令名必须准确描述其职责。
    - **动宾结构**：指令名应遵循 `动作-对象` 规范。严禁使用 `all`, `export`, `data` 等过于宽泛的单词。例如：使用 `sync-all` 而非 `all`，使用 `export-views` 而非 `export`。
5. **文档维护铁律 (Revision Integrity)**：在更新各级 `CLAUDE.md`、研报、跟踪手册或任何核心文档时，严禁为了当前更新目的而简化、省略或调整文档中存量无关内容。必须始终保持文档的物理完整性与细节密度，仅对目标内容进行手术式修订。**严禁在 `replace` 或 `write_file` 等操作中使用 `...`、`(rest of code)` 或 `(存量叙述保留)` 等任何形式的占位符，必须提供字面意义上的完整文本。**
6. **统一视图架构 (Unified View Architecture)**：
所有数据查询必须通过视图完成。视图采用 Python 类定义 (`storage/database/views/`)，支持显式依赖 (DAG) 与自动可视化。
7. **运行规范**：本项目统一使用 `uv` 进行环境管理。执行脚本格式：`uv run main.py <subcommand> [options]`。
8. **代码质量检查 (Lint Gate)**：修改核心代码（`storage/`、`backtest/`、`data_ingestion/`、`utils/`、`config/`、`analysis/`、`tools/`、`main.py`、`tests/`，不含 `workspace/`）后，必须依次运行 `uv run ruff check .` 与 `uv run ruff format --check .`，两项全部通过方可完成任务；如需格式化运行 `uv run ruff format .`。规则与豁免配置见 `pyproject.toml` 的 `[tool.ruff]`。
9. **测试覆盖要求 (Test Coverage Gate)**：代码变更必须有对应测试用例覆盖。修改核心代码（范围同 Lint Gate）后，须为变更的行为补充/更新单元测试（mock 网络与数据库，不触真实网络），并运行 `uv run pytest tests/` 确认全部通过方可完成任务；新增测试文件必须纳入提交，禁止出现"改代码无测试"的提交。测试规范：真实环境依赖（网络、外部接口、`data/` 数据湖）一律以 `monkeypatch` 或 mock 隔离，仅验证纯逻辑行为。
10. **禁止全量扫描数据目录 (Data Directory Guard)**：严禁使用 `ls` 或类似命令对 `data/` 目录进行全量或递归扫描（Parquet 分片数量巨大），必须使用精准路径、脚本查询或元数据库（`metadata.db`）定位目标。

## 4. 项目进度
- [x] **基础架构搭建**：完成 `uv` 环境配置，实现 SQLite/DuckDB 双引擎。
- [x] **存储架构升级 (Data Lake)**：实现 Parquet 分片存储与存算分离。
- [x] **统一视图架构**：实现 Code-as-Definition 模式，支持 DAG 加载与 PlantUML 拓扑可视化。
- [x] **股票元数据补全**：实现行业、地域、上市日期多源补全。
- [x] **财务/指标同步**：实现披露日历驱动的智能增量同步。
- [x] **财务 TTM 计算引擎**：实现自主计算无穿越的滚动财务指标。
- [x] **日线 K 线同步引擎**：实现增量行情采集与复权因子自动计算。
- [x] **CLI 系统重构**：实现基于子命令的标准化命令行入口。

## 5. Codex 任务路由
Codex 没有 OpenCode/Claude Code 的项目内斜杠命令自动展开机制。用户以自然语言提出任务时，必须按下列路由读取对应 workflow，并将 workflow 作为任务执行 SOP。

- **开发任务**：当用户要求修改、实现、重构、修复或排查系统功能时，读取 `docs/workflows/development.md`。
- **个股深度研究/重写/讨论**：当用户要求分析公司、重做标准研报或开启个股讨论时，读取 `docs/workflows/equity-research.md`；标准研报写完后必须按 `docs/workflows/research-review.md` 执行审查修正闭环。
- **个股研报独立复核/审查/修正**：当用户要求审查、复核、评估、质检或修正个股研报、跟踪手册或投研汇总时，先读取 `docs/workflows/equity-research.md`，再读取 `docs/workflows/research-review.md`；可按 workflow 使用运行时只读子 agent，并由主 agent 裁决后执行修正。
- **财报 PDF 转文本/信息提取**：当用户要求研读财报、提取年报/半年报/季报关键信息、或将财报 PDF 转为文本时，读取 `docs/workflows/financial-report-extraction.md`，并使用 Codex skill `$financial-report-extractor`。
- **跟踪审计日历批量巡检**：当用户要求检查 `investigation/equities/note.md` 中下次触发日已过、某个日期边界之前的触发事件是否发生、或批量判断是否需要进入完整跟踪更新时，读取 `docs/workflows/tracking-calendar-triage.md`。
- **个股动态同步/增量情报审计**：当用户要求同步某公司最新动态、公告、财报、关注点或市场信息时，读取 `docs/workflows/intelligence-sync.md`；若触发重大更新，继续读取 `docs/workflows/research-review.md` 并执行审查修正闭环。
- **行业或专题研报更新**：当用户要求更新 `investigation/` 下研报时，读取 `docs/workflows/report-update.md`。
- **Git 提交/推送**：当用户要求提交、生成 commit message 或推送时，读取 `docs/workflows/git-commit.md`。

若任务同时命中多个 workflow，应先说明执行顺序，再按最小必要集合执行。workflow 不得覆盖本文件中的硬约束；发生冲突时，以本文件为准。

## 6. 后续计划
- [x] **回测引擎基础建设**：实现基于统一视图、T+1 开盘成交与后复权净值计算的日频多策略回测框架。
- [ ] **因子库扩展**：利用 DuckDB 窗口函数在 SQL 层实现技术指标 (MA, MACD 等)。
- [ ] **自动化调度**：整合全流程同步任务。

## 7. 文档维护规范
- **功能完备性 (Definition of Done)**：
    1. **价值沉淀**：评估 `workspace/` 文档重要性。系统架构类沉淀至 `docs/`；研究结论类（如跟踪手册、报告）沉淀至 `investigation/`对应子目录。
    2. **文档演进**：优先更新现有文档以反映现状，仅在涉及全新领域时新建。
    3. **清理**：删除 `workspace/` 中的开发痕迹、临时脚本及过时设计方案。
    4. **规范同步**：确保 `usage.md` 等关键文档与最新代码逻辑、指令集保持一致。
    5. **精准提交**：执行 `git add` 必须精确到文件，严禁使用 `git add .`。
- **研报文件名同步 (Report Filename Sync)**：更新 `investigation/` 下任何研报内容时，必须同时更新：①文件内 `**日期**` 行主体日期为当前日期；②文件名结尾日期同步为当前日期。例如 `_2026年7月19日.md` → `_2026年7月22日.md`。
- **刷新机制**：修改本文件后，提醒用户重启会话或使用 `/init` 刷新上下文。

## 8. 其他工作规则
1. 所有用户指令与提问需要先规划执行步骤、然后全面思考关联信息，最后完整地完成任务。
2. **跟踪审计日历**：任何任务只要读取或更新 `investigation/equities/note.md` 的「跟踪审计日历」，必须先完整读取从 `## 跟踪审计日历` 标题至表头结束的内容，再按公司名定位行；严禁先用 `grep`、Glob 或局部读取命中公司行后，脱离表头规则直接更新。下次触发日仅可使用 `YYYY-MM-DD`、`YYYY-MM-DD~YYYY-MM-DD`、以 `; ` 分隔且按起始日升序的多个前述项目，或 `—`；不得使用不带年份的日期、自然语言时段、事件名称或括号说明。更新日历行后必须执行 `/sort-note` 校验格式并排序，不得手工猜测排序位置。
