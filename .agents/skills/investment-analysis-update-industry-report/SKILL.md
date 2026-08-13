---
name: investment-analysis-update-industry-report
description: Update a QuantPyLab industry research report from its tracking framework when the user provides a report path.
---

# 更新行业研报

将用户指定研报路径作为原命令的 `$ARGUMENTS`。在执行任何操作前，完整读取 `.opencode/commands/investmentAnalysis/updateIndustryReport.md`，并逐条执行其全部要求。

该文件是唯一流程基准。不得省略、合并、重排或改写阅读、检索、核验、更新、日期同步和汇报要求。`.opencode/` 只读，不得修改。

唯一替换：原命令中的 Exa MCP 调用改为 Codex 内置网络能力，且仅作以下等价替换：

- `web_search_exa`：执行内置网络搜索。
- `web_search_advanced_exa`：执行内置网络搜索，并在查询中保留原本的域名、时间范围和关键词限制。
- `web_fetch_exa`：打开并读取对应高价值链接的正文。

替换不改变检索词、信源层级、交叉验证、数据时点标注或未发布数据的处理规则。
