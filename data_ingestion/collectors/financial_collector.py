import akshare as ak
import pandas as pd
import time
import random
import os
from utils.logger import logger
from storage.database.indicator_store import IndicatorStore

class FinancialCollector:
    """
    财务数据采集器：负责三大报表及财务指标的抓取。
    """
    
    STATEMENTS = {
        "balance": "资产负债表",
        "profit": "利润表",
        "cashflow": "现金流量表"
    }

    def __init__(self):
        self.indicator_store = IndicatorStore()
        self.indicator_map = self._load_indicator_map()

    def _load_indicator_map(self) -> dict:
        """加载东财指标中英文映射字典"""
        # 优先查找 docs，后续可迁移至 config
        paths = ["docs/em_indicator_dict.csv", "workspace/em_indicator_dict.csv", "config/em_indicator_dict.csv"]
        mapping = {}
        
        for p in paths:
            if os.path.exists(p):
                try:
                    df = pd.read_csv(p)
                    # key -> name_cn
                    mapping = dict(zip(df['indicator_key'], df['name_cn']))
                    logger.info(f"成功加载指标字典: {len(mapping)} 条")
                    break
                except Exception as e:
                    logger.warning(f"加载字典 {p} 失败: {e}")
        
        return mapping

    def fetch_statement(self, code: str, stat_type: str) -> pd.DataFrame:
        """
        抓取指定股票的某类报表全量历史数据 (新浪源)。
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
            
            # 剔除新浪报表中的总分项前缀 (如 "其中:", "加:", "减:")
            # 新浪报表接口返回的是宽表，列名即为财务指标名称
            clean_map = {}
            import re
            for col in df.columns:
                if isinstance(col, str):
                    # 匹配 开头的 "其中[:：]", "加[:：]", "减[:：]"
                    new_col = re.sub(r'^(其中|加|减)[:：]', '', col).strip()
                    if new_col != col:
                        clean_map[col] = new_col
            
            if clean_map:
                df.rename(columns=clean_map, inplace=True)
                # 清洗后若产生重名列 (如 "现金" 和 "其中:现金" -> "现金"), 保留第一个
                df = df.loc[:, ~df.columns.duplicated()]

            if 'report_date' in df.columns:
                df = df.sort_values('report_date')

            return df

        except Exception:
            logger.exception(f"抓取 {code} {stat_name} 失败")
            return pd.DataFrame()

    def collect_indicators(self, symbol: str, market_symbol: str = None):
        """
        抓取并存储财务指标 (东财源)。
        :param symbol: 纯数字代码 (如 600004)
        :param market_symbol: 带后缀代码 (支持 sz300274 或 300274.SZ)
        """
        if not market_symbol:
            market_symbol = f"{symbol}.SH" if symbol.startswith('6') else f"{symbol}.SZ"

        # 格式标准化：统一转为 300274.SZ 这种东财标准格式
        if market_symbol.startswith(('sz', 'sh', 'bj', 'SZ', 'SH', 'BJ')):
            pre = market_symbol[:2].upper()
            suf = market_symbol[2:]
            market_symbol = f"{suf}.{pre}"
        else:
            market_symbol = market_symbol.upper()

        try:
            logger.info(f"正在从东财抓取指标: {market_symbol}")
            
            # 1. 调用东财接口
            df = ak.stock_financial_analysis_indicator_em(symbol=market_symbol, indicator="按报告期")
            
            if df is None or df.empty:
                logger.warning(f"{market_symbol} 接口返回为空，跳过")
                return

            # 2. 列名处理与翻译
            # 我们只保留在字典中定义了映射的列，以及必要的 symbol/report_date
            new_cols = {}
            keep_raw_cols = []
            
            # 报告期标准化前置处理
            report_date_col = '报告期' if '报告期' in df.columns else 'REPORT_DATE'
            if report_date_col in df.columns:
                df['report_date'] = pd.to_datetime(df[report_date_col]).dt.strftime('%Y%m%d')
                keep_raw_cols.append('report_date')
            else:
                logger.warning(f"{market_symbol} 数据格式异常，缺少报告期列")
                return

            # 映射中文列名
            for col in df.columns:
                if col in self.indicator_map:
                    cn_name = self.indicator_map[col]
                    # 1. 移除纯单位后缀
                    cn_name = cn_name.replace("(%)", "").replace("(元)", "").replace("(次)", "").replace("(天)", "")
                    # 2. 将剩余的括号转为下划线
                    cn_name = cn_name.replace("(", "_").replace(")", "").replace("（", "_").replace("）", "")
                    cn_name = cn_name.strip() # 彻底去除空格
                    new_cols[col] = cn_name
                    keep_raw_cols.append(col)
            
            # 3. 彻底剔除不需要的原始英文列 (只保留在 keep_raw_cols 里的)
            df = df[keep_raw_cols].copy()
            df.rename(columns=new_cols, inplace=True)
            
            # 4. 补充 symbol
            df['symbol'] = symbol 
            
            # 5. 入库
            self.indicator_store.save_indicators(df)

        except Exception:
            logger.exception(f"同步指标 {symbol} ({market_symbol}) 失败")

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
            except Exception:
                logger.warning(f"获取 {sym} 披露计划失败", exc_info=True)
        
        if not all_plans:
            return pd.DataFrame()
            
        df_combined = pd.concat(all_plans, ignore_index=True)
        # 统一列名
        df_combined.rename(columns={'股票代码': 'code', '实际披露时间': 'actual_date'}, inplace=True)
        return df_combined
