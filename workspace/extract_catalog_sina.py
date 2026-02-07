import akshare as ak
import pandas as pd
import re

def extract_sina_metadata():
    """
    提取新浪三大报表的列名及单位。
    """
    print("正在抓取新浪报表元数据...")
    symbol = "600004"
    
    tables = {
        "fin_balance_sheet": "资产负债表",
        "fin_income_statement": "利润表",
        "fin_cashflow_statement": "现金流量表",
        "fin_indicator_sina": "财务指标" # 用于对齐
    }
    
    all_records = []
    
    # 1. 三大报表
    for table_name, stat_name in list(tables.items())[:3]:
        try:
            df = ak.stock_financial_report_sina(stock=symbol, symbol=stat_name)
            # 这里的列名通常是：'资产' (行名), '2022-12-31' (列名)
            # 我们需要获取 index (如果是透视表) 或者 columns
            # 新浪报表接口返回的是指标在行上
            if '报告日' in df.columns:
                 # 这说明是纵表格式，指标在 row
                 # 但实际上我们要存的是宽表。
                 # 获取所有唯一的指标名
                 items = df.iloc[:, 0].unique().tolist()
                 for item in items:
                     all_records.append({
                         "table_name": table_name,
                         "column_name": item,
                         "original_name": item,
                         "unit": "元", # 会计报表默认单位
                         "source_api": "ak.stock_financial_report_sina",
                         "category": "报表项目"
                     })
        except Exception as e:
            print(f"抓取 {stat_name} 失败: {e}")

    # 2. 新浪指标 (这才是带单位最全的地方)
    try:
        df_ind = ak.stock_financial_analysis_indicator(symbol=symbol, start_year="2020")
        for col in df_ind.columns:
            if col == '日期': continue
            
            unit = "-"
            unit_match = re.search(r'\((%|元|次|天|元/股|倍)\)', col)
            if unit_match:
                unit = unit_match.group(1)
            
            # 记录下来，稍后用于补全东财
            all_records.append({
                "table_name": "fin_indicator_ref",
                "column_name": col.split("(")[0],
                "original_name": col,
                "unit": unit,
                "source_api": "ak.stock_financial_analysis_indicator",
                "category": "参考指标"
            })
    except Exception as e:
        print(f"抓取新浪指标失败: {e}")

    res_df = pd.DataFrame(all_records)
    res_df.to_csv("workspace/catalog_seed_sina.csv", index=False, encoding='utf-8-sig')
    print(f"成功提取 {len(res_df)} 个新浪元数据。")

if __name__ == "__main__":
    extract_sina_metadata()
