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
    """
    if code.startswith(('6', '9', '688')):
        return f"sh{code}"
    elif code.startswith(('0', '2', '3')):
        return f"sz{code}"
    elif code.startswith(('4', '8')):
        return f"bj{code}"
    return code
