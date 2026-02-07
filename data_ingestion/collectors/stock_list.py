import akshare as stock
import pandas as pd
from datetime import datetime
from utils.logger import logger

class StockListCollector:
    """
    股票列表采集器，负责从 AkShare 获取 A 股全量股票元数据。
    """

    def fetch_all_stocks(self) -> pd.DataFrame:
        """
        从 AkShare 获取全量 A 股代码和名称 (Phase 1)。
        """
        try:
            logger.info("正在从 AkShare 获取基础股票列表 (code, name)...")
            df = stock.stock_info_a_code_name()
            
            if df.empty:
                logger.warning("接口返回数据为空")
                return pd.DataFrame()

            # 1. 基础清洗
            df_cleaned = df.copy()
            # 确保 code 是 6 位字符串
            df_cleaned['code'] = df_cleaned['code'].astype(str).str.zfill(6)
            
            # 2. 生成 symbol
            df_cleaned['symbol'] = df_cleaned['code'].apply(self._generate_symbol)
            
            # 3. 字段补全 (对应数据库 Schema)
            df_cleaned['area'] = None
            df_cleaned['industry'] = None
            df_cleaned['list_date'] = None
            df_cleaned['is_active'] = 1
            df_cleaned['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 4. 去重与排序列 (确保与数据库字段顺序一致或明确列名)
            df_cleaned = df_cleaned.drop_duplicates(subset=['symbol'])
            
            # 最终输出列顺序对齐 Schema (可选，但在 append 模式下建议明确)
            final_cols = ['symbol', 'code', 'name', 'area', 'industry', 'list_date', 'is_active', 'updated_at']
            return df_cleaned[final_cols]

        except Exception as e:
            logger.error(f"同步股票列表失败: {str(e)}")
            return pd.DataFrame()

    def _generate_symbol(self, code: str) -> str:
        """根据代码首位生成带前缀的 symbol"""
        if code.startswith('6'):
            return f"sh{code}"
        elif code.startswith(('0', '3')):
            return f"sz{code}"
        elif code.startswith(('4', '8', '9')):
            return f"bj{code}"
        else:
            return f"unknown_{code}"

class StockDetailCollector:
    """
    个股详情采集器，负责补全行业、地域、上市日期等元数据。
    """
    
    def fetch_from_xueqiu(self, symbol: str) -> dict:
        """从雪球获取详情 (侧重地域和上市日期)"""
        try:
            # symbol 需为大写，如 SH600000
            df = stock.stock_individual_basic_info_xq(symbol=symbol.upper())
            if df.empty:
                return {}
            
            info = dict(zip(df['item'], df['value']))
            
            # 处理上市日期 (毫秒 -> YYYYMMDD)
            list_date = ""
            raw_date = info.get('listed_date')
            if raw_date and str(raw_date).isdigit():
                dt = datetime.fromtimestamp(int(raw_date) / 1000.0)
                list_date = dt.strftime('%Y%m%d')
            
            return {
                'area': info.get('provincial_name'),
                'list_date': list_date,
                'industry_xq': info.get('affiliate_industry', {}).get('ind_name') if isinstance(info.get('affiliate_industry'), dict) else None
            }
        except Exception as e:
            logger.debug(f"雪球接口抓取失败 {symbol}: {e}")
            return {}

    def fetch_from_eastmoney(self, code: str) -> dict:
        """从东财获取详情 (侧重行业)"""
        try:
            df = stock.stock_individual_info_em(symbol=code)
            if df.empty:
                return {}
            
            info = dict(zip(df['item'], df['value']))
            return {
                'industry': info.get('行业'),
                'list_date': info.get('上市时间')
            }
        except Exception as e:
            logger.debug(f"东财接口抓取失败 {code}: {e}")
            return {}
