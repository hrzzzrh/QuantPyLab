import akshare as ak
import pandas as pd
from datetime import datetime, date
from utils.logger import logger

def get_latest_trade_date(ref_date: datetime = None) -> date:
    """
    获取最近一个交易日的日期。
    如果今天是交易日且已经收盘(15:30后)，返回今天；
    如果是周末、节假日或今天尚未收盘，返回前一个最近的交易日。
    """
    if ref_date is None:
        ref_date = datetime.now()

    try:
        # 获取新浪财经的所有历史交易日
        # 该接口返回的是全量日期，包含未来预测（如 2026 年末）
        df = ak.tool_trade_date_hist_sina()
        trade_dates = pd.to_datetime(df['trade_date']).dt.date.tolist()
        
        # 过滤掉大于参考日期的部分
        past_trade_dates = [d for d in trade_dates if d <= ref_date.date()]
        
        if not past_trade_dates:
            return ref_date.date()
            
        latest = past_trade_dates[-1]
        
        # 特殊逻辑：如果是今天，但尚未收盘 (15:30)，则返回上一个交易日
        if latest == ref_date.date() and ref_date.hour < 15 or (ref_date.hour == 15 and ref_date.minute < 30):
            # 取倒数第二个
            if len(past_trade_dates) >= 2:
                return past_trade_dates[-2]
        
        return latest

    except Exception as e:
        logger.warning(f"获取交易日历失败: {e}，将返回当前系统日期作为兜底。")
        return ref_date.date()

if __name__ == "__main__":
    # 测试
    print(f"当前时间: {datetime.now()}")
    print(f"最近交易日: {get_latest_trade_date()}")
    
    # 模拟周六
    test_sat = datetime(2025, 2, 8, 10, 0)
    print(f"测试周六 (2025-02-08): {get_latest_trade_date(test_sat)}")
