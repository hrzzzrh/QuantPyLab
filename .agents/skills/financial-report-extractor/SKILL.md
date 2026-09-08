---
name: financial-report-extractor
description: 将个股财报 PDF 转换为文本并进行针对性信息提取，输出结构化提取笔记供深度研报使用。当用户要求研读财报、提取年报/半年报关键信息、或需要将财报 PDF 转文本时使用。
---

# 财报信息提取

将用户指定的财报 PDF 与提取主题清单作为输入。在执行任何操作前，完整读取 `.opencode/agents/financial-report-extractor.md`，并逐条执行其全部要求。

该文件是唯一流程基准。不得省略、合并、重排或改写其中的工作目录规范、执行流程、提取原则与完成标准。`.opencode/` 只读，不得修改。

唯一适配：原 agent 的子代理执行形态改为本 skill 在 ZCode 会话中直接执行。
