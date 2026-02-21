import akshare as ak
import pandas as pd
import os
from datetime import datetime, timedelta
from utils.logger import logger
from storage.file_store.parquet_store import ParquetStore
from storage.database.manager import db_manager

class ShareCollector:
    """
    股本变动数据采集器
    支持基于本地数据水位线的增量抓取。
    """

    def __init__(self):
        self.store = ParquetStore()

    def _get_local_max_date(self, symbol: str) -> str:
        """获取本地已存储的最新变动日期"""
        try:
            conn = db_manager.get_duckdb_conn()
            path = self.store.base_dir / "share_capital" / f"symbol={symbol}" / "data.parquet"
            if not path.exists():
                return "19900101"
            
            res = conn.execute(f"SELECT MAX(change_date) FROM read_parquet('{path}')").fetchone()
            if res and res[0]:
                if isinstance(res[0], datetime):
                    return res[0].strftime('%Y%m%d')
                return str(res[0]).replace('-', '')
            return "19900101"
        except Exception:
            return "19900101"

    def collect_share_capital(self, symbol: str, start_date: str = None):
        """
        同步股本变动记录
        :param symbol: 纯数字代码 (如 600519)
        :param start_date: 选填，强制抓取的起始日期 (YYYYMMDD)
        """
        if not start_date:
            local_max = self._get_local_max_date(symbol)
            if local_max == "19900101":
                start_date = "19900101"
            else:
                # 增量同步: 从本地最大日期的后一天开始
                dt = datetime.strptime(local_max, '%Y%m%d') + timedelta(days=1)
                start_date = dt.strftime('%Y%m%d')

        today = datetime.now().strftime('%Y%m%d')
        if start_date > today:
            logger.debug(f"{symbol} 股本数据已是最新，跳过")
            return

        try:
            logger.info(f"正在抓取股本变动: {symbol} ({start_date} -> {today})")

            # 1. 调用 AkShare 接口 (巨潮资讯源)
            try:
                df = ak.stock_share_change_cninfo(symbol=symbol, start_date=start_date, end_date=today)
            except KeyError as ke:
                if '公告日期' in str(ke):
                    logger.debug(f"{symbol} 在此期间无股本变动 (触发 akshare KeyError)")
                    return
                raise ke

            if df is None or df.empty:
                logger.debug(f"{symbol} 在此期间无股本变动")
                return

            # 2. 清洗与转换
            df_cleaned = df[['变动日期', '总股本']].copy()
            df_cleaned.rename(columns={
                '变动日期': 'change_date',
                '总股本': 'total_shares'
            }, inplace=True)

            # 日期归一化
            df_cleaned['change_date'] = pd.to_datetime(df_cleaned['change_date']).dt.date
            
            # 单位转换 (万股 -> 股)
            df_cleaned['total_shares'] = df_cleaned['total_shares'].astype(float) * 10000
            df_cleaned['symbol'] = symbol

            # 3. 增量合并逻辑
            self._save_incremental(df_cleaned, symbol)

        except Exception:
            logger.exception(f"抓取股本变动 {symbol} 失败")

    def _save_incremental(self, df_new: pd.DataFrame, symbol: str):
        """增量合并并保存"""
        path = self.store.base_dir / "share_capital" / f"symbol={symbol}" / "data.parquet"
        
        if path.exists():
            df_old = pd.read_parquet(path)
            # 统一日期格式以便去重 (df_old 的日期读取后可能是 datetime.date 或 Timestamp)
            df_old['change_date'] = pd.to_datetime(df_old['change_date']).dt.date
            df_new['change_date'] = pd.to_datetime(df_new['change_date']).dt.date
            
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            # 按日期去重，保留最新的一条
            df_combined.drop_duplicates(subset=['change_date'], keep='last', inplace=True)
            df_combined.sort_values('change_date', inplace=True)
        else:
            df_combined = df_new

        self.store.save_partition(df_combined, 'share_capital', symbol)
        logger.info(f"股本变动保存成功: {symbol} ({len(df_combined)} 条记录)")

if __name__ == "__main__":
    # 测试
    collector = ShareCollector()
    collector.collect_share_capital("600519")
