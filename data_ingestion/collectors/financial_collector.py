import akshare as ak
import pandas as pd
import time
import random
from utils.logger import logger

class FinancialCollector:
    """
    财务报表采集器：从新浪获取三大报表原始数据。
    """
    
    STATEMENTS = {
        "balance": "资产负债表",
        "profit": "利润表",
        "cashflow": "现金流量表"
    }

    def fetch_statement(self, code: str, stat_type: str) -> pd.DataFrame:
        """
        抓取指定股票的某类报表全量历史数据。
        """
        stat_name = self.STATEMENTS.get(stat_type)
        if not stat_name:
            raise ValueError(f"无效的报表类型: {stat_type}")

        try:
            logger.debug(f"正在从新浪抓取 {code} 的 {stat_name}...")
            df = ak.stock_financial_report_sina(stock=code, symbol=stat_name)
            
            if df.empty:
                return pd.DataFrame()

            # 基础清洗
            df['symbol'] = code 
            if '报告日' in df.columns:
                df.rename(columns={'报告日': 'report_date'}, inplace=True)
            
            if 'report_date' in df.columns:
                df = df.sort_values('report_date')

            return df

        except Exception as e:
            logger.error(f"抓取 {code} {stat_name} 失败: {e}")
            return pd.DataFrame()

    def get_disclosure_plans(self, date: str) -> pd.DataFrame:
        """
        获取指定报告期的全市场披露计划 (包含沪深京)。
        date: 格式如 '20251231'
        """
        all_plans = []
        # 组合查询：沪深A股 + 京市A股
        for sym in ['沪深A股', '京市A股']:
            try:
                logger.info(f"正在获取 {sym} 的 {date} 披露计划...")
                df = ak.stock_yysj_em(symbol=sym, date=date)
                if not df.empty:
                    all_plans.append(df)
            except Exception as e:
                logger.warning(f"获取 {sym} 披露计划失败: {e}")
        
        if not all_plans:
            return pd.DataFrame()
            
        df_combined = pd.concat(all_plans, ignore_index=True)
        # 统一列名
        df_combined.rename(columns={'股票代码': 'code', '实际披露时间': 'actual_date'}, inplace=True)
        return df_combined
