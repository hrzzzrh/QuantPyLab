import akshare as ak
import pandas as pd
import random
from datetime import datetime, timedelta
from utils.logger import logger
from utils.retry import retry
from utils.trade_date import get_latest_trade_date
from utils.financial import to_sina_symbol, get_market_label, MarketLabel
from storage.file_store.parquet_store import ParquetStore
from storage.database.manager import db_manager

class DailyKlineCollector:
    """
    日线 K 线采集器
    策略: 存原始价格 + 复权因子
    支持多源切换 (Sina/EM)
    """

    def __init__(self, source: str = "em"):
        self.store = ParquetStore()
        self.source = source

    def _get_local_max_date(self, symbol: str) -> str:
        """获取本地已存储的最新日期"""
        try:
            conn = db_manager.get_duckdb_conn()
            # 尝试从视图中查询，如果视图未建立或表为空则返回远古日期
            # 注意: daily_kline 视图可能尚未在 DBManager 中定义，这里直接读文件
            path = self.store.base_dir / "daily_kline" / f"symbol={symbol}" / "data.parquet"
            if not path.exists():
                return "19900101"
            
            res = conn.execute(f"SELECT MAX(date) FROM read_parquet('{path}')").fetchone()
            if res and res[0]:
                # DuckDB 返回 date 类型或 ISO 字符串
                if isinstance(res[0], datetime):
                    return res[0].strftime('%Y%m%d')
                return str(res[0]).replace('-', '')
            return "19900101"
        except Exception:
            return "19900101"

    def _fetch_from_em(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从东方财富抓取行情 (原逻辑)"""
        # 1. 抓取不复权数据
        df_raw = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
        if df_raw.empty:
            return pd.DataFrame()

        # 2. 抓取后复权数据 (用于计算因子)
        df_hfq = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")
        
        # 3. 对齐并计算 adj_factor
        rename_map = {
            '日期': 'date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount'
        }
        df_raw = df_raw[list(rename_map.keys())].rename(columns=rename_map)
        df_hfq = df_hfq[['日期', '收盘']].rename(columns={'日期': 'date', '收盘': 'close_hfq'})
        
        df_merge = pd.merge(df_raw, df_hfq, on='date', how='left')
        df_merge['adj_factor'] = df_merge['close_hfq'] / df_merge['close']
        df_merge['adj_factor'] = df_merge['adj_factor'].ffill().fillna(1.0)
        
        return df_merge

    def _fetch_from_sina(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从新浪财经抓取行情 (新逻辑)"""
        sina_symbol = to_sina_symbol(symbol)
        
        # 补丁: 新浪接口对 UA 敏感，akshare 内部请求可能缺失 UA 导致断开连接
        from unittest.mock import patch
        import requests
        
        original_get = requests.get
        def patched_get(*args, **kwargs):
            if 'headers' not in kwargs or not kwargs['headers']:
                kwargs['headers'] = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
                }
            return original_get(*args, **kwargs)

        with patch('requests.get', side_effect=patched_get):
            # 1. 抓取不复权数据
            df_raw = ak.stock_zh_a_daily(symbol=sina_symbol, start_date=start_date, end_date=end_date, adjust="")
            if df_raw.empty:
                return pd.DataFrame()

            # 2. 抓取后复权数据
            df_hfq = ak.stock_zh_a_daily(symbol=sina_symbol, start_date=start_date, end_date=end_date, adjust="hfq")
        
        # 3. 标准化处理
        # 新浪接口列名已经是英文: date, open, high, low, close, volume, amount, ...
        # 注意: 新浪 volume 单位是股，本项目统一为手
        df_raw['volume'] = df_raw['volume'] / 100.0
        
        df_raw = df_raw[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        df_hfq = df_hfq[['date', 'close']].rename(columns={'close': 'close_hfq'})
        
        # 确保日期格式一致以便合并
        df_raw['date'] = pd.to_datetime(df_raw['date']).dt.strftime('%Y-%m-%d')
        df_hfq['date'] = pd.to_datetime(df_hfq['date']).dt.strftime('%Y-%m-%d')
        
        df_merge = pd.merge(df_raw, df_hfq, on='date', how='left')
        
        # 计算因子
        df_merge['adj_factor'] = df_merge['close_hfq'] / df_merge['close']
        df_merge['adj_factor'] = df_merge['adj_factor'].ffill().fillna(1.0)
        
        return df_merge

    @retry(max_retries=2, delay=2.0)
    def collect_kline(self, symbol: str, start_date: str = None, end_date: str = None):
        """
        同步日线行情
        :param symbol: 纯数字代码 (如 600519)
        """
        # 获取最新的有效交易日作为基准
        latest_trade_date = get_latest_trade_date().strftime('%Y%m%d')

        if not end_date:
            end_date = latest_trade_date

        if not start_date:
            local_max = self._get_local_max_date(symbol)
            if local_max == "19900101":
                start_date = "19900101"
            else:
                # 增量同步: 从本地最大日期的后一天开始
                dt = datetime.strptime(local_max, '%Y%m%d') + timedelta(days=1)
                start_date = dt.strftime('%Y%m%d')

        if start_date > end_date:
            logger.debug(f"{symbol} 已是最新 (目标: {end_date})，无需同步")
            return

        # --- 智能多源决策 ---
        market = get_market_label(symbol)
        if market == MarketLabel.BJ:
            active_source = "sina"
        else:
            active_source = random.choice(["em", "sina"])

        try:
            logger.debug(f"正在从 {active_source} 抓取行情: {symbol} ({start_date} -> {end_date})")
            
            if active_source == "sina":
                df_merge = self._fetch_from_sina(symbol, start_date, end_date)
            else:
                df_merge = self._fetch_from_em(symbol, start_date, end_date)

            if df_merge.empty:
                logger.warning(f"{symbol} 抓取数据为空 (Source: {active_source})")
                return

            # 最终清洗
            df_merge['symbol'] = symbol
            df_merge['date'] = pd.to_datetime(df_merge['date']).dt.date # 转为 Python date 对象
            
            final_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor', 'symbol']
            df_final = df_merge[final_cols].copy()

            # 5. 存储 (增量合并逻辑)
            self._save_incremental(df_final, symbol)

        except Exception as e:
            # Re-raise 让 @retry 装饰器捕捉并重试
            raise e

    def _save_incremental(self, df_new: pd.DataFrame, symbol: str):
        """增量合并并保存"""
        path = self.store.base_dir / "daily_kline" / f"symbol={symbol}" / "data.parquet"
        
        if path.exists():
            df_old = pd.read_parquet(path)
            # 简单追加并去重
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=['date'], keep='last', inplace=True)
            df_combined.sort_values('date', inplace=True)
        else:
            df_combined = df_new

        self.store.save_partition(df_combined, 'daily_kline', symbol)
        logger.debug(f"行情保存成功: {symbol} ({len(df_combined)} 条记录)")

if __name__ == "__main__":
    # 测试
    collector = DailyKlineCollector()
    collector.collect_kline("600519", start_date="20240101")
