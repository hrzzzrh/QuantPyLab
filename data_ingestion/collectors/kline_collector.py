import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from utils.logger import logger
from storage.file_store.parquet_store import ParquetStore
from storage.database.manager import db_manager

class DailyKlineCollector:
    """
    日线 K 线采集器
    策略: 存原始价格 + 复权因子
    """

    def __init__(self):
        self.store = ParquetStore()

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

    def collect_kline(self, symbol: str, start_date: str = None, end_date: str = None):
        """
        同步日线行情
        :param symbol: 纯数字代码 (如 600519)
        """
        if not start_date:
            local_max = self._get_local_max_date(symbol)
            if local_max == "19900101":
                start_date = "19900101"
            else:
                # 增量同步: 从本地最大日期的后一天开始
                dt = datetime.strptime(local_max, '%Y%m%d') + timedelta(days=1)
                start_date = dt.strftime('%Y%m%d')

        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')

        if start_date > end_date:
            logger.info(f"{symbol} 已是最新，无需同步")
            return

        try:
            logger.info(f"正在抓取行情: {symbol} ({start_date} -> {end_date})")
            
            # 1. 抓取不复权数据
            df_raw = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
            if df_raw.empty:
                logger.warning(f"{symbol} 不复权数据为空")
                return

            # 2. 抓取后复权数据 (用于计算因子)
            df_hfq = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")
            
            # 3. 对齐并计算 adj_factor
            # 统一列名映射
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
            
            # 合并
            df_merge = pd.merge(df_raw, df_hfq, on='date', how='left')
            
            # 计算因子: adj_factor = close_hfq / close
            # 处理停牌或数据异常导致的 0 除
            df_merge['adj_factor'] = df_merge['close_hfq'] / df_merge['close']
            df_merge['adj_factor'] = df_merge['adj_factor'].ffill().fillna(1.0)
            
            # 4. 最终清洗
            df_merge['symbol'] = symbol
            df_merge['date'] = pd.to_datetime(df_merge['date']).dt.date # 转为 Python date 对象，Parquet 友好
            
            final_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor', 'symbol']
            df_final = df_merge[final_cols].copy()

            # 5. 存储 (增量合并逻辑)
            self._save_incremental(df_final, symbol)

        except Exception as e:
            logger.error(f"抓取行情 {symbol} 失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())

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
        logger.info(f"行情保存成功: {symbol} ({len(df_combined)} 条记录)")

if __name__ == "__main__":
    # 测试
    collector = DailyKlineCollector()
    collector.collect_kline("600519", start_date="20240101")
