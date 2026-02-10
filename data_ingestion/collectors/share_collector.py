import akshare as ak
import pandas as pd
from utils.logger import logger
from storage.file_store.parquet_store import ParquetStore

class ShareCollector:
    """
    股本变动数据采集器
    """

    def __init__(self):
        self.store = ParquetStore()

    def collect_share_capital(self, symbol: str):
        """
        获取并存储指定股票的股本变动记录
        :param symbol: 纯数字代码 (如 600519)
        """
        try:
            logger.info(f"正在抓取股本变动: {symbol}")
            
            # 1. 调用 AkShare 接口 (巨潮资讯源)
            df = ak.stock_share_change_cninfo(symbol=symbol)
            
            if df is None or df.empty:
                logger.warning(f"{symbol} 股本变动数据为空")
                return

            # 2. 清洗与转换
            # 目标列: change_date, total_shares
            # 原始列: 变动日期, 总股本
            df_cleaned = df[['变动日期', '总股本']].copy()
            df_cleaned.rename(columns={
                '变动日期': 'change_date',
                '总股本': 'total_shares'
            }, inplace=True)

            # 日期归一化 (YYYY-MM-DD -> YYYY-MM-DD)
            df_cleaned['change_date'] = pd.to_datetime(df_cleaned['change_date']).dt.strftime('%Y-%m-%d')
            
            # 单位转换 (万股 -> 股)
            df_cleaned['total_shares'] = df_cleaned['total_shares'].astype(float) * 10000
            
            # 补充 symbol 并排序
            df_cleaned['symbol'] = symbol
            df_cleaned.sort_values('change_date', ascending=True, inplace=True)
            
            # 去重 (同一天可能有多次变动，取最后一次)
            df_cleaned.drop_duplicates(subset=['change_date'], keep='last', inplace=True)

            # 3. 存储
            self.store.save_partition(df_cleaned, 'share_capital', symbol)
            logger.info(f"股本变动保存成功: {symbol} ({len(df_cleaned)} 条记录)")

        except Exception as e:
            logger.error(f"抓取股本变动 {symbol} 失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())

if __name__ == "__main__":
    # 测试
    collector = ShareCollector()
    collector.collect_share_capital("600519")
