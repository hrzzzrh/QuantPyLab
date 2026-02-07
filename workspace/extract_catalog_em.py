import akshare as ak
import pandas as pd
import re
import os

def extract_em_metadata():
    """
    回溯东财接口原始列名，解析单位和分类。
    """
    print("正在回溯东财接口原始元数据...")
    symbol = "600004.SH"
    
    try:
        df = ak.stock_financial_analysis_indicator_em(symbol=symbol, indicator="按报告期")
        
        dict_path = "workspace/em_indicator_dict.csv"
        if not os.path.exists(dict_path):
            print("Error: Missing indicator dict")
            return
            
        mapping_df = pd.read_csv(dict_path)
        key_to_name = dict(zip(mapping_df['indicator_key'], mapping_df['name_cn']))
        
        records = []
        for col_key in df.columns:
            raw_name = key_to_name.get(col_key, col_key)
            
            unit = "-"
            unit_match = re.search(r'\((%|元|次|天|元/股|倍|%|天/次)\)', raw_name)
            if unit_match:
                unit = unit_match.group(1)
            
            col_name = raw_name.replace("(%)", "").replace("(元)", "").replace("(次)", "").replace("(天)", "").replace("(元/股)", "").replace("(倍)", "")
            col_name = col_name.replace("(", "_").replace(")", "").replace("（", "_").replace("）", "").strip()
            
            if col_key in ['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'REPORT_DATE', 'NOTICE_DATE', 'UPDATE_DATE']:
                continue

            records.append({
                "table_name": "fin_indicator",
                "column_name": col_name,
                "original_name": raw_name,
                "unit": unit,
                "source_api": "ak.stock_financial_analysis_indicator_em",
                "category": "财务指标"
            })
            
        res_df = pd.DataFrame(records)
        res_df.to_csv("workspace/catalog_seed_em.csv", index=False, encoding='utf-8-sig')
        print(f"Success: Extracted {len(res_df)} items.")
        print(res_df[['column_name', 'unit']].head(10).to_string())
        
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    extract_em_metadata()
