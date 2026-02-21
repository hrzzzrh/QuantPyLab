import requests
import json
import time
import random
from tqdm import tqdm
from utils.logger import logger
from .headers import EM_HEADERS, EM_PARAMS_COMMON

class HighSpeedIndustryCollector:
    """
    高效行业采集器：利用东财原生批量接口，快速刷新全市场行业分类。
    """
    
    BASE_URL = "https://17.push2.eastmoney.com/api/qt/clist/get"

    def sync_industries(self, db_conn):
        """
        主流程：
        1. 获取所有行业板块列表
        2. 遍历每个行业，获取成份股
        3. 更新数据库
        """
        try:
            # 1. 获取行业列表
            industries = self._fetch_industry_list()
            logger.info(f"获取到 {len(industries)} 个行业板块，开始同步成份股...")
            
            cursor = db_conn.cursor()
            total_updated = 0
            
            # 2. 遍历每个行业
            pbar = tqdm(industries, desc="同步行业成份股", unit="行业")
            for idx, ind in enumerate(pbar):
                bk_code = ind.get('f12')  # 板块ID，如 BK0475
                bk_name = ind.get('f14')  # 板块名称，如 银行
                
                if not bk_code:
                    continue
                    
                pbar.set_postfix({"当前": bk_name})
                stocks = self._fetch_constituents(bk_code)
                
                if not stocks:
                    continue
                
                # 3. 批量更新
                # 注意：这里我们只更新 industry 字段，不插入新股票，以免破坏 stocks 表的基础
                # 如果某只股票不在 stocks 表里（可能是新上市），暂且忽略，交由 Phase 1 基础同步处理
                updates = [(bk_name, s.get('f12')) for s in stocks]
                
                cursor.executemany("UPDATE stocks SET industry = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?", updates)
                total_updated += cursor.rowcount
                
                # 提交事务并休眠
                if idx % 5 == 0:
                    db_conn.commit()
                
                time.sleep(random.uniform(0.3, 0.5)) # 保持温和
                
            db_conn.commit()
            logger.info(f"行业同步完成！累计更新了 {total_updated} 条记录的行业信息。")
            
        except Exception:
            logger.exception("行业同步过程中断")
            if 'db_conn' in locals():
                db_conn.rollback()

    def _fetch_industry_list(self):
        """获取所有行业板块 ID"""
        params = EM_PARAMS_COMMON.copy()
        params.update({
            "fs": "m:90+t:2+f:!50",
            "fields": "f12,f14",
            "pn": "1",
            "pz": "200", # 行业数一般不超过 100
            "fid": "f3"
        })
        
        headers = EM_HEADERS.copy()
        headers["referer"] = "https://quote.eastmoney.com/center/gridlist.html"
        
        return self._request_em_api(params, headers)

    def _fetch_constituents(self, bk_code):
        """获取指定板块的成份股"""
        params = EM_PARAMS_COMMON.copy()
        params.update({
            "fs": f"b:{bk_code}+f:!50",
            "fields": "f12,f14",
            "pn": "1",
            "pz": "2000", # 假设一个行业不超过 2000 只股票
            "fid": "f3"
        })
        
        headers = EM_HEADERS.copy()
        headers["referer"] = "https://quote.eastmoney.com/center/boardlist.html" # 关键差异
        
        return self._request_em_api(params, headers)

    def _request_em_api(self, params, headers):
        try:
            response = requests.get(self.BASE_URL, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            # 处理 JSON 或 JSONP
            text = response.text.strip()
            if text.startswith("jQuery"):
                # 提取括号内容
                start = text.find("(") + 1
                end = text.rfind(")")
                text = text[start:end]
            
            data = json.loads(text)
            if data and "data" in data and "diff" in data["data"]:
                return data["data"]["diff"]
            return []
            
        except Exception:
            logger.warning("请求东财接口失败", exc_info=True)
            return []
