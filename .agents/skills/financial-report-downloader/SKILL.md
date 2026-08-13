---
name: financial-report-downloader
description: 下载中国深交所（SZSE）和上交所（SSE）上市公司的定期财报（年报、半年报、季报）。支持指定股票代码、下载目录及文件数量。
---

# Financial Report Downloader

## 概述
本技能通过调用深交所和上交所的公开查询接口，获取并下载指定股票的财报 PDF 文件。它能够自动识别股票代码所属的交易所，并按顺序下载最新的报告。

## 触发场景
- 当用户要求下载某只股票的财报时。
- 当用户需要获取特定公司最近几年的年度或季度报告时。

## 使用指南

### 1. 识别交易所与股票代码
- **深交所 (SZSE)**：代码以 `00` 或 `30` 开头（如 `000001`, `300274`）。
- **上交所 (SSE)**：代码以 `60`, `68` 或 `90` 开头（如 `601899`, `688001`）。
- **其他**：对于其他前缀，请回复“待进一步开发支持”。

### 2. 执行下载任务
本技能脚本包含 PEP 723 依赖声明，推荐使用 `uv run` 执行，它会自动处理 `requests` 依赖，无需手动安装。

**命令格式：**
```bash
uv run .agents/skills/financial-report-downloader/scripts/download_reports.py <股票代码> <下载目录> [数量]
```

**参数逻辑：**
- `<股票代码>`：必须提供的 6 位数字代码。
- `<下载目录>`：用户指定的存放路径（如 `./workspace/sunpower_reports`）。
- `[数量]`（可选）：
    - 如果用户指定了范围（如“下载最近 5 份”），则传入对应数字。
    - 如果用户未明确范围，**默认传入 `12`**。

### 3. 示例工作流
- **请求**：“帮我下载阳光电源 (300274) 的财报，放到 workspace/sunpower 目录。”
- **指令**：`uv run .agents/skills/financial-report-downloader/scripts/download_reports.py 300274 workspace/sunpower 12`

- **请求**：“下载紫金矿业 (601899) 最近 3 份报表到 reports 文件夹。”
- **指令**：`uv run .agents/skills/financial-report-downloader/scripts/download_reports.py 601899 reports 3`

## 注意事项
- **零配置运行**：脚本自包含所有工具函数，且通过 PEP 723 声明了依赖。
- **环境要求**：只需系统安装有 `uv` 和 `python` 即可运行，不依赖当前项目的 `pyproject.toml`。
- **结果输出**：如果脚本运行没有输出下载到的文件，可能是因为指定目录已经存在对应文件，请注意检查
