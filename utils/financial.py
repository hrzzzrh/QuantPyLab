def get_previous_report_date(report_date: str) -> str:
    """
    获取前一个标准的财务报告期。
    YYYYMMDD -> YYYYMMDD (3/31, 6/30, 9/30, 12/31)
    """
    year = int(report_date[:4])
    md = report_date[4:]
    
    mapping = {
        "0331": (year - 1, "1231"),
        "0630": (year, "0331"),
        "0930": (year, "0630"),
        "1231": (year, "0930")
    }
    
    new_year, new_md = mapping.get(md, (year, md))
    return f"{new_year}{new_md}"

def get_consecutive_reports(end_report: str, n: int = 5) -> list:
    """
    获取以 end_report 为结束点的连续 N 个报告期列表
    """
    reports = [end_report]
    current = end_report
    for _ in range(n - 1):
        current = get_previous_report_date(current)
        reports.append(current)
    return reports

def to_sina_symbol(code: str) -> str:
    """
    将 6 位数字代码转换为新浪格式 (带 sh/sz/bj 前缀)
    
    规则参考:
    - sh: 6 (主板/科创板), 900 (B股), 5 (基金/ETF), 000 (指数)
    - sz: 0 (主板), 3 (创业板), 2 (B股), 1 (基金/ETF)
    - bj: 4, 8, 920 (北交所)
    """
    if code.startswith(('6', '5')):
        return f"sh{code}"
    if code.startswith(('0', '2', '3', '1')):
        return f"sz{code}"
    if code.startswith(('4', '8')):
        return f"bj{code}"
    if code.startswith('9'):
        if code.startswith('920'):
            return f"bj{code}"
        return f"sh{code}"  # 900 为沪市 B 股
    return code
