import akshare as ak
import pandas as pd
from datetime import datetime, date
import os
from pathlib import Path
from functools import lru_cache
from utils.logger import logger
from config.settings import WAREHOUSE_DIR

CACHE_FILE = Path(WAREHOUSE_DIR) / "metadata" / "trade_calendar.parquet"

@lru_cache(maxsize=1)
def _get_all_trade_dates() -> list[date]:
    """获取所有交易日历列表（带缓存）"""
    need_update = False
    
    if CACHE_FILE.exists():
        try:
            df = pd.read_parquet(CACHE_FILE)
            trade_dates = pd.to_datetime(df['trade_date']).dt.date.tolist()
            
            # 检查缓存是否足够新
            # 如果最新日期早于今天，可能需要更新（akshare 通常提供未来日期）
            today = date.today()
            if trade_dates[-1] < today:
                need_update = True
                logger.info("交易日历缓存可能已过期，准备从网络同步...")
            else:
                return trade_dates
        except Exception as e:
            logger.warning(f"读取交易日历缓存失败: {e}")
            need_update = True
    else:
        need_update = True
        logger.info("未发现交易日历本地缓存，准备初始化...")

    if need_update:
        try:
            # 获取新浪财经的所有历史交易日
            df = ak.tool_trade_date_hist_sina()
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            trade_dates = sorted(df['trade_date'].tolist())
            
            # 持久化到本地
            os.makedirs(CACHE_FILE.parent, exist_ok=True)
            pd.DataFrame({'trade_date': trade_dates}).to_parquet(CACHE_FILE, index=False)
            logger.info(f"交易日历已同步并缓存至: {CACHE_FILE}")
            
            return trade_dates
        except Exception as e:
            logger.error(f"从网络获取交易日历失败: {e}")
            if CACHE_FILE.exists():
                logger.info("将使用旧的本地缓存作为兜底。")
                return pd.to_datetime(pd.read_parquet(CACHE_FILE)['trade_date']).dt.date.tolist()
            raise

def get_latest_trade_date(ref_date: datetime = None) -> date:
    """
    获取最近一个交易日的日期。
    如果今天是交易日且已经收盘(15:30后)，返回今天；
    如果是周末、节假日或今天尚未收盘，返回前一个最近的交易日。
    """
    if ref_date is None:
        ref_date = datetime.now()

    try:
        trade_dates = _get_all_trade_dates()
        
        # 过滤掉大于参考日期的部分
        past_trade_dates = [d for d in trade_dates if d <= ref_date.date()]
        
        if not past_trade_dates:
            return ref_date.date()
            
        latest = past_trade_dates[-1]
        
        # 特殊逻辑：如果是今天，但尚未收盘 (15:30)，则返回上一个交易日
        if latest == ref_date.date() and (ref_date.hour < 15 or (ref_date.hour == 15 and ref_date.minute < 30)):
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
