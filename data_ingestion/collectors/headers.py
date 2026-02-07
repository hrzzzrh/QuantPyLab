# 东财 API 专用 Header 配置
# 注意：Cookie 和 ut 参数可能会随时间失效，若请求失败请更新此处

EM_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7,vi;q=0.6",
    "connection": "keep-alive",
    "cookie": "fullscreengg=1; fullscreengg2=1; qgqp_b_id=870947b6a314f95b131c3316de84baba; st_nvi=mh7iAeGvGJv1zigBFm_-L6374; st_si=55882658602641; st_asi=delete; nid18=0f512d6ee90e691d53d979bde12a1561; nid18_create_time=1770389148072; gviem=rYgB01V2kJSXW3RxPQtxs91d4; gviem_create_time=1770389148073; wsc_checkuser_ok=1; st_pvi=01626098699592; st_sp=2026-02-06%2022%3A45%3A47; st_inirUrl=https%3A%2F%2Fquote.eastmoney.com%2Fcenter%2Fboardlist.html; st_sn=4; st_psi=20260206224853433-113200313002-7287963993",
    "host": "17.push2.eastmoney.com",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "script",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
}

EM_PARAMS_COMMON = {
    "np": "1",
    "fltt": "1",
    "invt": "2",
    "po": "1",
    "dect": "1",
    "wbp2u": "|0|0|0|web",
    "ut": "fa5fd1943c7b386f172d6893dbfba10b"  # 你的第一个 ut，经测试可用
}
