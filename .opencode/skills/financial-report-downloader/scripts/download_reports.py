#!/usr/bin/env python3
# /// script
# dependencies = [
#   "requests",
# ]
# ///

import sys
import os
import json
import re
import requests
import time
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# --- 工具函数 ---
def get_session(headers=None):
    session = requests.Session()
    default_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
    }
    if headers:
        default_headers.update(headers)
    session.headers.update(default_headers)
    return session

def random_sleep(min_sec=0.5, max_sec=1.5):
    time.sleep(random.uniform(min_sec, max_sec))

def is_real_report(title):
    if not any(k in title for k in ['年度报告', '半年度报告', '季度报告']):
        return False
    black_list = [
        '摘要', '公告', '英文', 'English', '说明会', '修订稿', 
        '补充', '专项说明', '自愿性', '问询', '函', '监管', '提示',
        '意见', '关于'
    ]
    if any(b in title for b in black_list):
        return False
    clean_title = title.strip()
    if not (clean_title.endswith('报告') or re.search(r'报告\s*$', clean_title)):
        return False
    return True

def download_file(session, url, save_path, referer):
    try:
        if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
            return True
        random_sleep(1.0, 2.0)
        headers = {'Referer': referer}
        response = session.get(url, stream=True, timeout=60, headers=headers)
        response.raise_for_status()
        
        # 检查是否又是 HTML (反爬)
        content_type = response.headers.get('Content-Type', '').lower()
        if 'html' in content_type:
            print(f"❌ Failed to download {save_path}: Received HTML instead of PDF (Anti-crawling).")
            return False

        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # 再次检查文件头
        with open(save_path, 'rb') as f:
            header = f.read(4)
            if header != b"%PDF":
                print(f"❌ Failed to download {save_path}: Invalid PDF header.")
                os.remove(save_path)
                return False

        print(f"✅ Downloaded: {os.path.basename(save_path)}")
        return True
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        return False

# --- 巨潮资讯 (Cninfo) 逻辑 ---
def get_org_id(session, stock_code):
    """通过股票代码获取巨潮内部 orgId"""
    search_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    # 简单推断板块
    if stock_code.startswith(('60', '68', '90')):
        column = "sse"
    elif stock_code.startswith(('00', '20', '30')):
        column = "szse"
    elif stock_code.startswith(('4', '8')):
        column = "bj"
    else:
        column = "sse" # 默认沪市

    payload = {
        "pageNum": 1,
        "pageSize": 10,
        "tabName": "fulltext",
        "column": column,
        "stock": "",
        "searchkey": stock_code,
        "isCheckee": "false",
    }
    headers = {
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/fulltext/search",
        "X-Requested-With": "XMLHttpRequest"
    }
    try:
        resp = session.post(search_url, data=payload, headers=headers, timeout=15)
        data = resp.json()
        announcements = data.get('announcements', [])
        for a in announcements:
            if a.get('secCode') == stock_code:
                return a.get('orgId'), column
    except Exception as e:
        print(f"⚠️ Failed to fetch orgId for {stock_code}: {e}")
    return None, column

def fetch_cninfo_reports(stock_code, limit=12):
    print(f"🔍 Searching Cninfo for {stock_code} (target {limit})...")
    search_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    
    session = get_session()
    org_id, column = get_org_id(session, stock_code)
    
    if not org_id:
        print(f"⚠️ Could not find precise orgId for {stock_code}, using heuristic.")
        if stock_code.startswith(('60', '68', '90')):
            full_stock = f"{stock_code},gssh0{stock_code}"
            column = "sse"
        else:
            full_stock = f"{stock_code},gssz0{stock_code}"
            column = "szse"
    else:
        full_stock = f"{stock_code},{org_id}"

    results = []
    seen_urls = set()
    page_num = 1
    
    # 上交所接口在 pageSize=30 时翻页最稳定
    # 最多查看 20 页以确保找到足够的财报
    while len(results) < limit and page_num <= 20:
        payload = {
            "pageNum": page_num,
            "pageSize": 30, # 必须设为 30，否则 SH 股票翻页会失效
            "tabName": "fulltext",
            "column": column,
            "stock": full_stock,
            "searchkey": "报告",
            "category": "category_ndbg;category_bndbg;category_yjdbg;category_sjdbg",
            "isCheckee": "false",
            "showNext": "true"
        }
        
        headers = {
            "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/fulltext/search",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        try:
            resp = session.post(search_url, data=payload, headers=headers, timeout=30)
            data = resp.json()
            announcements = data.get('announcements', [])
            if not announcements:
                break
                
            found_on_page = 0
            for item in announcements:
                title = item['announcementTitle']
                url = f"http://static.cninfo.com.cn/{item['adjunctUrl']}"
                
                if url not in seen_urls and is_real_report(title):
                    results.append({
                        'title': title,
                        'url': url,
                        'date': item.get('announcementTime', '')
                    })
                    seen_urls.add(url)
                    found_on_page += 1
                    if len(results) >= limit:
                        break
            
            # 如果这一页完全没有新东西（去重后），可能已经翻不动了
            if found_on_page == 0 and page_num > 1 and not announcements:
                break

            page_num += 1
            random_sleep(0.3, 0.8)
        except Exception as e:
            print(f"⚠️ Cninfo fetch error: {e}")
            break
            
    print(f"✅ Total valid reports found: {len(results)}")
    return results[:limit]

# --- 深交所 (SZSE) 逻辑 ---
def fetch_szse_reports(stock_code, limit=12):
    print(f"🔍 Searching SZSE for {stock_code}...")
    url = "https://www.szse.cn/api/disc/announcement/annList"
    session = get_session({'Content-Type': 'application/json', 'Referer': 'https://www.szse.cn/'})
    payload = {
        "stock": [stock_code],
        "channelCode": ["listedNotice_disc"],
        "bigCategoryId": ["010307", "010305", "010303", "010301"],
        "pageSize": 50,
        "pageNum": 1
    }
    try:
        resp = session.post(f"{url}?random={random.random()}", json=payload, timeout=30)
        data = resp.json()
        results = []
        for item in data.get('data', []):
            title = item['title']
            if is_real_report(title):
                results.append({
                    'title': title,
                    'url': "https://disc.static.szse.cn/download" + item['attachPath'],
                    'date': item.get('publishTime', '')
                })
        return results[:limit]
    except Exception as e:
        print(f"⚠️ SZSE search error: {e}")
        return []

def main(stock_code, target_dir, limit=12):
    # 优先使用巨潮资讯 (Cninfo)，因为它涵盖两市且 PDF 下载反爬相对较弱
    reports = fetch_cninfo_reports(stock_code, limit)
    
    # 如果巨潮没找到，深交所股票可以尝试深交所官网
    if not reports and stock_code.startswith(('00', '30', '20')):
        reports = fetch_szse_reports(stock_code, limit)

    if not reports:
        print("No reports found or failed to bypass anti-crawling.")
        return

    os.makedirs(target_dir, exist_ok=True)
    session = get_session()
    
    # 串行下载或低并发，防止触发 WAF
    with ThreadPoolExecutor(max_workers=2) as executor:
        for r in reports:
            clean_title = re.sub(r'[\\/:*?"<>|]', '_', r['title']).strip()
            save_path = os.path.join(target_dir, f"{stock_code}_{clean_title}.pdf")
            # 根据 URL 决定 Referer
            referer = "http://www.cninfo.com.cn/" if "cninfo" in r['url'] else "https://www.szse.cn/"
            executor.submit(download_file, session, r['url'], save_path, referer)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: uv run download_reports.py <code> <dir> [limit]")
    else:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 12)
