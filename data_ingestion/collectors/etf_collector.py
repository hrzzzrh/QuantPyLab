import akshare as ak
import pandas as pd
import requests
import time
from datetime import datetime
from typing import Optional
from utils.logger import logger
from utils.retry import retry
from storage.file_store.parquet_store import ParquetStore
from storage.database.manager import db_manager


class ETFListCollector:
    """
    ETF列表采集器，负责从东财获取场内交易基金列表
    """
    
    def fetch_all_etfs(self) -> pd.DataFrame:
        """
        从东财获取全量ETF列表（包含代码、名称、类型、成立日期等）
        """
        try:
            logger.info("正在从东财获取ETF基金列表...")
            df = ak.fund_exchange_rank_em()
            
            if df.empty:
                logger.warning("ETF列表接口返回数据为空")
                return pd.DataFrame()
            
            df_cleaned = df.copy()
            
            df_cleaned['symbol'] = df_cleaned['基金代码'].astype(str).str.zfill(6)
            df_cleaned['code'] = df_cleaned['symbol']
            df_cleaned['name'] = df_cleaned['基金简称']
            df_cleaned['fund_type'] = df_cleaned['类型']
            
            if '成立日期' in df_cleaned.columns:
                df_cleaned['list_date'] = pd.to_datetime(df_cleaned['成立日期'], errors='coerce').dt.strftime('%Y%m%d')
            else:
                df_cleaned['list_date'] = None
            
            df_cleaned['is_active'] = 1
            df_cleaned['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            df_cleaned = df_cleaned.drop_duplicates(subset=['symbol'])
            
            final_cols = ['symbol', 'code', 'name', 'fund_type', 'list_date', 'is_active', 'updated_at']
            return df_cleaned[final_cols]
            
        except Exception:
            logger.exception("同步ETF列表失败")
            return pd.DataFrame()


class ETFKlineCollector:
    """
    ETF日线K线采集器
    数据源: 雪球API（支持复权）
    策略: 存原始价格 + 复权因子（参考股票K线逻辑）
    """
    
    def __init__(self):
        self.store = ParquetStore()
        self.cookies = self._load_cookies()
        self.base_url = "https://stock.xueqiu.com/v5/stock/chart/kline.json"
        
    def _load_cookies(self) -> dict:
        """从环境变量或配置加载雪球Cookie"""
        import os
        
        cookies = {}
        
        xq_a_token = os.getenv('XQ_A_TOKEN')
        xq_r_token = os.getenv('XQ_R_TOKEN')
        xq_id_token = os.getenv('XQ_ID_TOKEN')
        
        if xq_a_token:
            cookies['xq_a_token'] = xq_a_token
        if xq_r_token:
            cookies['xq_r_token'] = xq_r_token
        if xq_id_token:
            cookies['xq_id_token'] = xq_id_token
        
        if not cookies:
            cookies = {
                'cookiesu': '411770389947101',
                'device_id': '9d310ce1b7714b83e85daad7b63130ff',
                's': 'be11igit1l',
                'xq_a_token': '20458f74230aee45906ecb90d8c70ff43daa3837',
                'xqat': '20458f74230aee45906ecb90d8c70ff43daa3837',
                'xq_r_token': 'fa5fac8aea31fef0733c31a1c3670554e9365bda',
                'xq_id_token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTc4MTA1NDU5NiwiY3RtIjoxNzc5MTEzMDczNjE2LCJjaWQiOiJkOWQwbjRBWnVwIn0.JnfA8EBblOlG4UxiNekEW8hz2qoWUVlouw5e3c0SD8AvsgUvBFboxxSNOU_PNEv7zaH1yZHk4Dam3pq1aFKKa2jerz6_zi-xayyiEiIyMIyMcZXaB7etsqMNimakZXftBhFJf3sI6wc1hf3dNtVtkEioMy2Lc2tjWPKsXbxhHUOF8yw-V1bv-opUenlh68jMuNnLA3lzFJtU2TeS6OhZXyficZ9UZqFjTiwOYKQXd58A1pA09cT-3QbXLJ0hfIUJuAeSiQ0XEWzX2KkIQ1qwYzoWcHu6wZboHfa9FfgPVe3Z5ZeRiFZFWIxXFU-EKMt2BtXt0oO4IkGzIH3pytkDlQ',
                'u': '411770389947101'
            }
            logger.warning("使用硬编码Cookie（仅用于测试），生产环境请配置环境变量")
        
        return cookies
    
    def _get_xueqiu_symbol(self, code: str) -> str:
        """
        将6位代码转换为雪球格式
        51/50/51开头 -> SH
        15/16开头 -> SZ
        """
        if code.startswith(('51', '50', '58')):
            return f"SH{code}"
        elif code.startswith(('15', '16', '18')):
            return f"SZ{code}"
        else:
            return f"SH{code}"
    
    def _get_local_max_date(self, symbol: str) -> str:
        """获取本地已存储的最新日期"""
        try:
            path = self.store.base_dir / "etf_kline" / f"symbol={symbol}" / "data.parquet"
            if not path.exists():
                return "19900101"
            
            conn = db_manager.get_duckdb_conn()
            res = conn.execute(f"SELECT MAX(date) FROM read_parquet('{path}')").fetchone()
            if res and res[0]:
                if isinstance(res[0], datetime):
                    return res[0].strftime('%Y%m%d')
                return str(res[0]).replace('-', '')
            return "19900101"
        except Exception:
            return "19900101"
    
    @retry(max_retries=3, delay=2.0)
    def _fetch_from_xueqiu(self, symbol: str, type_param: str, count: int = -10000) -> pd.DataFrame:
        """
        从雪球获取K线数据
        
        Args:
            symbol: 雪球格式代码（如SH510050）
            type_param: normal（不复权）/ after（后复权）
            count: 获取数据条数（负数表示向前获取，默认-10000获取全部历史）
        """
        begin = int(time.time() * 1000)
        
        params = {
            'symbol': symbol,
            'begin': begin,
            'period': 'day',
            'type': type_param,
            'count': count,
            'indicator': 'kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            'Referer': f'https://xueqiu.com/S/{symbol}',
            'Origin': 'https://xueqiu.com'
        }
        
        resp = requests.get(
            self.base_url,
            params=params,
            headers=headers,
            cookies=self.cookies,
            timeout=15
        )
        
        if resp.status_code != 200:
            raise Exception(f"雪球接口返回状态码: {resp.status_code}")
        
        data = resp.json()
        
        if 'data' not in data:
            raise Exception(f"雪球接口返回数据格式异常: {data.get('error_description', 'Unknown error')}")
        
        # 兼容 items 和 item 两种字段名
        items = data['data'].get('item') or data['data'].get('items', [])
        if not items:
            return pd.DataFrame()
        
        # 只取前10列（K线基础数据）
        items_kline = [item[:10] for item in items]
        
        columns = ['timestamp', 'volume', 'open', 'high', 'low', 'close', 'chg', 'percent', 'turnoverrate', 'amount']
        
        df = pd.DataFrame(items_kline, columns=columns)
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        return df
    
    def collect_kline(self, symbol: str, start_date: str = None, end_date: str = None):
        """
        同步ETF日线行情
        
        Args:
            symbol: 纯数字代码（如510050）
            start_date: 起始日期（YYYYMMDD）
            end_date: 结束日期（YYYYMMDD）
        """
        xq_symbol = self._get_xueqiu_symbol(symbol)
        
        if not start_date:
            local_max = self._get_local_max_date(symbol)
            start_date = local_max
        
        try:
            logger.debug(f"正在从雪球抓取不复权数据: {symbol}")
            df_normal = self._fetch_from_xueqiu(xq_symbol, type_param='normal', count=-10000)
            
            if df_normal.empty:
                logger.warning(f"{symbol} 不复权数据为空")
                return
            
            time.sleep(0.5)
            
            logger.debug(f"正在从雪球抓取后复权数据: {symbol}")
            df_after = self._fetch_from_xueqiu(xq_symbol, type_param='after', count=-10000)
            
            if df_after.empty:
                logger.warning(f"{symbol} 后复权数据为空")
                return
            
            df_merged = pd.merge(
                df_normal[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']],
                df_after[['date', 'close']].rename(columns={'close': 'close_hfq'}),
                on='date',
                how='left'
            )
            
            df_merged['adj_factor'] = df_merged['close_hfq'] / df_merged['close']
            df_merged['adj_factor'] = df_merged['adj_factor'].ffill().fillna(1.0)
            
            df_merged['symbol'] = symbol
            df_merged['date'] = df_merged['date'].dt.date
            
            if start_date != "19900101":
                start_dt = datetime.strptime(start_date, '%Y%m%d').date()
                df_merged = df_merged[df_merged['date'] > start_dt]
            
            if df_merged.empty:
                logger.debug(f"{symbol} 无新增数据需要同步")
                return
            
            final_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor', 'symbol']
            df_final = df_merged[final_cols].copy()
            
            self._save_incremental(df_final, symbol)
            
        except Exception as e:
            logger.error(f"ETF K线抓取失败: {symbol}, 错误: {e}")
            raise e
    
    def _save_incremental(self, df_new: pd.DataFrame, symbol: str):
        """增量合并并保存"""
        path = self.store.base_dir / "etf_kline" / f"symbol={symbol}" / "data.parquet"
        
        if path.exists():
            df_old = pd.read_parquet(path)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=['date'], keep='last', inplace=True)
            df_combined.sort_values('date', inplace=True)
        else:
            df_combined = df_new
        
        self.store.save_partition(df_combined, 'etf_kline', symbol)
        logger.debug(f"ETF K线保存成功: {symbol} ({len(df_combined)} 条记录)")


if __name__ == "__main__":
    collector = ETFKlineCollector()
    collector.collect_kline("510050")
