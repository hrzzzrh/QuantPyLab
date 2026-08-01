---
name: deep-researcher
description: 深度调研助理。用于执行多步、自主的信息搜集与高质量投研报告生成。适用于对特定行业、个股、宏观专题或技术方案进行详尽调研。它能够自动拆解问题、进行多维度并行搜索、全文抓取高价值网页、处理数据冲突并产出结构化 Markdown 报告。
---

# Deep Researcher

## 核心能力
本技能旨在模拟深度研究功能，通过多步推理和循环检索，为复杂问题提供深度、客观且结构化的分析。

## 触发条件
- 当用户要求"深度调研"、"分析行业趋势"或"生成投研报告"时。
- 当用户提出的问题涉及多个未知的市场数据、政策变动或技术细节时。
- 当用户需要对比全球范围内的信息（需要中英文双语检索）时。

## 动态调研算法 (The Planning-First Research Loop)

本技能的核心是**"规划 → 搜索 → 评估 → 动态修正"**。在获得用户指令后，严禁直接搜索。

### Step 0：制定研究路线图 (Research Roadmap)
- **要求**：在执行任何搜索之前，必须先输出一份详细的调研计划（基于 `references/research_framework.md` 的模版）。
- **内容**：核心假设、3-5 个关键调研支柱、中英文搜索词、成功标准。

### Step 1：执行与评估循环 (The Loop: Act -> Assess -> Update Roadmap)

#### 动作 (Act)
- 根据路线图中的优先级，并发使用 Exa 搜索工具（`web_search_exa` 常规检索、`web_search_advanced_exa` 限定品类/域名/发布时间、`web_fetch_exa` 抓取高价值页面全文）。

#### 评估与更新 (Assess & Update)
- **已核实 (Verified)**：勾选路线图中已解决的项。
- **知识缺口 (Gaps)**：识别出哪些部分证据不足或存在冲突。
- **动态修正 (Roadmap Update)**：
  - 如果发现新变量，**立即在路线图中增加新的调研支柱**。
  - 如果原支柱证伪，**标记为已放弃并解释原因**。

#### 决策 (Decide)
- 继续追击路线图中的剩余项或新发现的支柱。
- 当且仅当路线图的核心项均已标记为"已核实"或"不可寻"，才退出循环。

### Step 2：最终合成 (Final Synthesis)
- 根据最新的路线图，按照标准模版输出最终报告。

### 3. 退出条件 (Definition of Done)
- [ ] 所有核心数据项（Market Size, CAGR, Prices 等）已获取。
- [ ] 关键风险点已识别并评估。
- [ ] 至少对比了中、英双语环境下的核心观点（针对国际化课题）。
- [ ] 报告中的每一条核心陈述都有对应的 `[Source URI]`。

## 报告生成 (Final Synthesis)
- 只有在满足退出条件后，才按照 `references/research_framework.md` 的模版输出最终报告。

## 示例应用

### 请求：调研全球低空经济的现状与中国政策导向
1. **拆解**：定义、全球市场规模、eVTOL 技术进展、中国三部委最新政策、基础设施瓶颈。
2. **搜索**：`Low-altitude economy 2024 report`, `eVTOL certification status FAA EASA`, `中国低空经济 2026 规划`。
3. **抓取**：抓取工信部官网、FAA 官网、亿航/小鹏汇天 IR 页面。
4. **合成**：生成包含摘要、三维度深度分析、风险评估及引用的完整报告。

## 资源清单
- [research_framework.md](references/research_framework.md)：标准报告模版与搜索协议。
- [data_confidence.md](references/data_confidence.md)：数据置信度评分与冲突处理机制。