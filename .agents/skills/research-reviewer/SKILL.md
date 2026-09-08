---
name: research-reviewer
description: 独立审查 investigation/ 下研报、跟踪手册与投研汇总，核验其是否符合 deep_investment_analysis_framework.md 框架要求，并识别乐观偏差、遗漏利空、逻辑缺陷与事实错误。当用户要求审查、复核或评估研报及跟踪手册质量时使用。
---

# 研报独立审查

将用户指定的审查目标作为输入。在执行任何操作前，完整读取 `.opencode/agents/research-reviewer.md`，并逐条执行其全部要求。

该文件是唯一流程基准。不得省略、合并、重排或改写其中的审查前置准备、审查独立性铁律、审查维度与输出规范。`.opencode/` 只读，不得修改。

唯一适配：原 agent 的子代理执行形态改为本 skill 在 ZCode 会话中直接执行；网络核验优先使用已接入的 Exa MCP 工具（`web_search_exa` / `web_search_advanced_exa` / `web_fetch_exa`）。
