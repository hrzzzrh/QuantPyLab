# 工具函数库 (Utilities)

本文档记录 QuantPyLab 中封装的通用工具函数，供开发者在采集、分析和回测逻辑中调用。

---

## 1. 交易日历 (`utils/trade_date.py`)

### `get_latest_trade_date`
获取最近一个已收盘的交易日日期。

- **功能**: 
    - 自动识别周末和节假日（返回上一个交易日）。
    - 自动判断收盘时间：如果今天是交易日，但在 **15:30** 之前调用，仍会返回上一个交易日，以确保数据已完全收盘可抓取。
- **输入**: 
    - `ref_date` (datetime, 可选): 参考时间，默认为当前系统时间。
- **输出**: 
    - `date` (datetime.date): 最近交易日对象。
- **示例**:
```python
from utils.trade_date import get_latest_trade_date

# 假设今天是 2025-02-08 (周六)
ld = get_latest_trade_date()
print(ld) # 输出: 2025-02-07
```

---

## 2. 日志工具 (`utils/logger.py`)

### `logger`
全局统一的日志对象。

- **功能**: 同时输出到控制台 (Console) 和文件 (`logs/app.log`)。
- **级别**: 默认 INFO，可根据需要调整。
- **示例**:
```python
---

## 3. 财务报告期工具 (`utils/financial.py`)

### `get_previous_report_date`
推算上一个标准的季度报告期（3/31, 6/30, 9/30, 12/31）。

### `get_consecutive_reports`
获取连续的 $N$ 个报告期列表。常用于校验财务数据的完整性。
- **场景**: 用于 TTM 计算前的“数据齐全度”自检。
- **示例**:
```python
from utils.financial import get_consecutive_reports
# 获取 2025Q3 及其之前的共 5 个季度
list = get_consecutive_reports("20250930", 5)
# 输出: ['20250930', '20250630', '20250331', '20241231', '20240930']
```
