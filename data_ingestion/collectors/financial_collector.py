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
        stat_type: 'balance', 'profit', 'cashflow'
        """
        stat_name = self.STATEMENTS.get(stat_type)
        if not stat_name:
            raise ValueError(f"无效的报表类型: {stat_type}")

        try:
            logger.debug(f"正在从新浪抓取 {code} 的 {stat_name}...")
            # 新浪接口返回的是 DataFrame，列名为中文
            df = ak.stock_financial_report_sina(stock=code, symbol=stat_name)
            
            if df.empty:
                return pd.DataFrame()

            # 基础清洗
            # 1. 统一 symbol 字段
            # 我们在 stocks 表里存的是 6 位 code 或 symbol，这里统一加上市场前缀
            # 注意：新浪接口输入是 6 位 code
            df['symbol'] = code # 暂时存 6 位，写入时可对齐
            
            # 2. 格式化报告日 (新浪返回 20240331)
            if '报告日' in df.columns:
                df.rename(columns={'报告日': 'report_date'}, inplace=True)
            
            # 3. 排序 (按日期从小到大，方便后续分析)
            if 'report_date' in df.columns:
                df = df.sort_values('report_date')

            return df

        except Exception as e:
            logger.error(f"抓取 {code} {stat_name} 失败: {e}")
            return pd.DataFrame()
