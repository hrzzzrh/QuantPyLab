# Financial Report Extraction Workflow

本 workflow 用于将个股财报 PDF 转换为文本，并按指定主题提取客观披露信息，供后续深度研报或情报同步使用。

## 入口判断

当用户要求研读财报、提取年报/半年报/季报关键信息、将财报 PDF 转文本、或从财报中提取产品、收入、产能、研发、客户供应商、风险因素等主题时，使用本 workflow。

## 执行顺序

1. 先读取 `docs/data_catalog.md`、`docs/data_schema.md`、`docs/view_architecture.md` 与 `docs/usage.md`，确认项目数据与查询约束。
2. 使用 Codex 全局 skill `$financial-report-extractor` 执行 PDF 转文本与结构化提取。
3. 若任务同时涉及下载最新财报，先使用 `$financial-report-downloader` 下载披露 PDF，再使用 `$financial-report-extractor` 提取。
4. 若提取结果将用于个股深度研究、情报同步或研报更新，再继续读取对应 workflow。

## 输入确认

从用户指令中确认：

1. 公司名称与股票代码。
2. 财报 PDF 的精确路径；若未给出路径，优先在 `workspace/research/<公司名称>/financial_reports/` 下精准定位。
3. 需要提取的信息主题清单。
4. 输出是否仅作为工作区材料，还是需要进一步进入 `investigation/` 研究资产。

## 输出位置

1. 财报 PDF 存放于 `workspace/research/<公司名称>/financial_reports/`。
2. 转换后的纯文本存放于 `workspace/research/<公司名称>/tmp_data/`。
3. 提取笔记存放于 `workspace/research/<公司名称>/tmp_data/`。
4. 文件名必须精确描述公司、报告期、报告类型与内容用途，禁止使用 `data.txt`、`output.md` 等模糊命名。

## 提取边界

1. 只提取客观披露事实、原始数字与原始单位，不做投资分析判断。
2. 若主题未披露，明确标注“未披露”，禁止编造。
3. 不修改原始 PDF。
4. 不越过用户主题清单扩展提取范围，除非用户明确要求。
5. 若需要量化核对，必须通过 `storage/database/views/` 下定义的视图完成。

## 完成标准

1. 转换文本文件与提取笔记均已落盘。
2. 提取笔记覆盖全部指定主题。
3. 最终回复包含处理文件清单、关键数据摘要与异常说明。
