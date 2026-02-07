# QuantPyLab

基于 Python 的 A 股量化交易实验室，实现数据采集、分析与回测全流程。

## 快速开始

### 1. 环境准备
项目使用 `uv` 进行包管理。
```bash
# 安装依赖
uv sync
```

### 2. 数据初始化
同步全量 A 股股票列表元数据：
```bash
uv run main.py --sync-stocks
```

补全股票详细元数据（行业、地域、上市日期）：
```bash
uv run main.py --enrich-metadata
```

同步 A 股全量财务报表（三大表）：
```bash
uv run main.py --sync-fin
```

同步 A 股财务指标（ROE、增长率、周转率等）：
```bash
uv run main.py --sync-indicators
```

## 项目结构
请参阅 `docs/architecture.md` 了解架构详情。
详细命令说明请参阅 `docs/usage.md`。
