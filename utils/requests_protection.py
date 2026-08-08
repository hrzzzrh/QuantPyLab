"""
统一的新浪数据源请求保护层。

通过 monkey-patch `requests.sessions.Session.request` 为 akshare 内部所有
新浪接口请求注入伪装头 (随机 UA/Referer)、周期性冷却，并识别 IP 风控
(HTTP 456 / 拦截页) 抛出 SinaBlockedError 实现快速止损。

- 仅对白名单中的新浪域名生效，不影响东财/雪球等其他数据源请求。
- 需在同步流程启动前调用 `install_requests_protection()` 安装。
"""

import random
import time
from urllib.parse import urlparse

import requests

from utils.logger import logger


class SinaBlockedError(RuntimeError):
    """新浪财经接口 IP 风控封禁，重试无意义，必须中止。"""


SINA_DOMAINS = (
    "quotes.sina.cn",
    "vip.stock.finance.sina.com.cn",
    "finance.sina.com.cn",
    "hq.sinajs.cn",
)

SINA_BROWSER_HEADERS_POOL = [
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/138.0.0.0 Safari/537.36"
        ),
        "Referer": "https://vip.stock.finance.sina.com.cn/",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Referer": "https://vip.stock.finance.sina.com.cn/",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.4 Safari/605.1.15"
        ),
        "Referer": "https://vip.stock.finance.sina.com.cn/",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
    },
]

# 每 N 次新浪请求冷却 N 秒 (周期性降温, 降低风控触发概率)
_COOL_DOWN_INTERVAL = 50
_COOL_DOWN_SECONDS = 10
_sina_request_count = 0
_installed = False


def _is_sina_url(url: str) -> bool:
    host = urlparse(url).hostname or ""
    return any(host == d or host.endswith("." + d) for d in SINA_DOMAINS)


def _is_sina_blocked_page(resp) -> bool:
    """识别新浪风控拦截页: HTTP 456, 或非 JSON 的 HTML 拦截页 (含"拒绝访问/封禁"特征)。"""
    if resp.status_code == 456:
        return True
    ctype = resp.headers.get("Content-Type", "")
    if "json" in ctype.lower():
        return False
    body = resp.content[:2000].decode("gbk", errors="ignore")
    return "拒绝访问" in body or "封禁" in body


def install_requests_protection() -> None:
    """为 akshare 内部所有新浪请求安装伪装/冷却/风控止损层 (幂等)。"""
    global _installed
    if _installed:
        return
    _installed = True

    original_request = requests.sessions.Session.request

    def protected_request(self, method, url, *args, **kwargs):
        global _sina_request_count
        if _is_sina_url(url):
            # 周期性冷却
            _sina_request_count += 1
            if _sina_request_count % _COOL_DOWN_INTERVAL == 0:
                logger.info(
                    f"新浪接口已达 {_sina_request_count} 次请求，冷却 {_COOL_DOWN_SECONDS}s..."
                )
                time.sleep(_COOL_DOWN_SECONDS)

            # 注入伪装头 (不覆盖调用方显式传入的字段)
            headers = dict(kwargs.get("headers") or {})
            for key, value in random.choice(SINA_BROWSER_HEADERS_POOL).items():
                headers.setdefault(key, value)
            kwargs["headers"] = headers

        resp = original_request(self, method, url, *args, **kwargs)

        if _is_sina_url(url) and _is_sina_blocked_page(resp):
            raise SinaBlockedError(
                f"新浪接口对当前 IP 触发风控 (HTTP {resp.status_code})，封禁约 5~60 分钟后自动解除"
            )
        return resp

    requests.sessions.Session.request = protected_request
    logger.info("已安装新浪数据源请求保护层 (伪装头/冷却/IP 风控止损)")
