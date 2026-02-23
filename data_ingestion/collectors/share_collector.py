import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from utils.logger import logger
from storage.file_store.parquet_store import ParquetStore
from storage.database.manager import db_manager

class ShareCollector:
    """
    股本变动数据采集器 (新浪财经源)
    支持基于本地数据水位线的增量抓取。
    """

    def __init__(self):
        self.store = ParquetStore()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def _get_local_max_date(self, symbol: str) -> str:
        """获取本地已存储的最新变动日期"""
        try:
            conn = db_manager.get_duckdb_conn()
            path = self.store.base_dir / "share_capital" / f"symbol={symbol}" / "data.parquet"
            if not path.exists():
                return "1990-01-01"
            
            res = conn.execute(f"SELECT MAX(change_date) FROM read_parquet('{path}')").fetchone()
            if res and res[0]:
                if isinstance(res[0], (datetime, pd.Timestamp)):
                    return res[0].strftime('%Y-%m-%d')
                return str(res[0])
            return "1990-01-01"
        except Exception:
            return "1990-01-01"

    def _fetch_sina_share_capital(self, symbol: str) -> pd.DataFrame:
        """
        解析新浪财经股本变动页面
        """
        url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructureHistory/stockid/{symbol}/stocktype/TotalStock.phtml"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'gbk'
            if response.status_code != 200:
                logger.error(f"请求新浪股本页面失败: {symbol}, Status: {response.status_code}")
                return pd.DataFrame()

            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all('table', id=re.compile(r'^historyTable'))
            
            data = []
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        date_str = tds[0].get_text(strip=True)
                        shares_str = tds[1].get_text(strip=True)
                        
                        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                            shares_match = re.search(r'([\d\.]+)万股', shares_str)
                            if shares_match:
                                # 转换为股并使用 int64
                                shares = int(round(float(shares_match.group(1)) * 10000))
                                data.append({'change_date': date_str, 'total_shares': shares})
            
            df = pd.DataFrame(data)
            if not df.empty:
                df['change_date'] = pd.to_datetime(df['change_date']).dt.date
                df['symbol'] = symbol
                df = df.sort_values('change_date', ascending=True).drop_duplicates('change_date')
            return df
        except Exception as e:
            logger.error(f"解析新浪股本变动 {symbol} 出错: {e}")
            return pd.DataFrame()

    def collect_share_capital(self, symbol: str, start_date: str = None):
        """
        同步股本变动记录
        :param symbol: 纯数字代码 (如 600519)
        :param start_date: 选填，强制抓取的起始日期 (YYYY-MM-DD)
        """
        local_max = self._get_local_max_date(symbol)
        if not start_date:
            start_date = local_max

        logger.info(f"正在抓取股本变动: {symbol} (本地最新: {local_max})")

        # 1. 抓取新浪数据 (新浪返回的是全量历史)
        df_new = self._fetch_sina_share_capital(symbol)
        
        if df_new is None or df_new.empty:
            logger.debug(f"{symbol} 未获取到股本变动记录")
            return

        # 2. 过滤增量数据
        # 支持 YYYYMMDD 和 YYYY-MM-DD 格式
        if isinstance(start_date, str):
            start_date = start_date.replace('-', '')
            try:
                start_dt = datetime.strptime(start_date, '%Y%m%d').date()
            except ValueError:
                logger.error(f"无效的日期格式: {start_date}，请使用 YYYYMMDD 或 YYYY-MM-DD")
                return
        else:
            start_dt = start_date

        df_filtered = df_new[df_new['change_date'] > start_dt].copy()

        if df_filtered.empty:
            logger.info(f"{symbol} 股本数据已是最新，跳过")
            return

        # 3. 增量合并逻辑
        self._save_incremental(df_filtered, symbol)

    def _save_incremental(self, df_new: pd.DataFrame, symbol: str):
        """增量合并并保存"""
        path = self.store.base_dir / "share_capital" / f"symbol={symbol}" / "data.parquet"
        
        if path.exists():
            df_old = pd.read_parquet(path)
            # 确保类型一致
            df_old['change_date'] = pd.to_datetime(df_old['change_date']).dt.date
            
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=['change_date'], keep='last', inplace=True)
            df_combined.sort_values('change_date', inplace=True)
        else:
            df_combined = df_new

        # 强制转换为 int64，避免 float64 污染
        try:
            df_combined['total_shares'] = df_combined['total_shares'].astype(float).round().astype('int64')
        except Exception as e:
            logger.warning(f"转换 {symbol} 股本为 int64 失败: {e}，尝试强制转换")
            df_combined['total_shares'] = pd.to_numeric(df_combined['total_shares'], errors='coerce').fillna(0).astype('int64')
        
        self.store.save_partition(df_combined, 'share_capital', symbol)
        logger.info(f"股本变动保存成功: {symbol} ({len(df_combined)} 条记录)")

if __name__ == "__main__":
    collector = ShareCollector()
    collector.collect_share_capital("300059")
