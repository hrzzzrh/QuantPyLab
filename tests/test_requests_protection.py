"""单元测试: utils/requests_protection.py 新浪请求保护层"""

from unittest import mock

import pytest
import requests

from utils.requests_protection import (
    SinaBlockedError,
    _is_sina_url,
    install_requests_protection,
)


def fake_response(status_code=200, content_type="application/json", content=b"{}"):
    resp = mock.Mock()
    resp.status_code = status_code
    resp.headers = {"Content-Type": content_type}
    resp.content = content
    return resp


class TestIsSinaUrl:
    def test_sina_domains(self):
        for url in [
            "https://quotes.sina.cn/cn/api/openapi.php/x",
            "https://vip.stock.finance.sina.com.cn/corp/go.php/x",
            "https://hq.sinajs.cn/list=sh600519",
        ]:
            assert _is_sina_url(url)

    def test_non_sina_domains(self):
        for url in [
            "https://datacenter-web.eastmoney.com/api/data/v1/get",
            "https://xueqiu.com/x",
            "https://www.baidu.com/x",
        ]:
            assert not _is_sina_url(url)


class TestProtectionLayer:
    def setup_method(self):
        # 强制重装以隔离测试状态
        import utils.requests_protection as rp

        rp._installed = False

    def test_inject_headers_on_sina_url(self, monkeypatch):
        calls = []

        def fake_request(self, method, url, *args, **kwargs):
            calls.append(kwargs)
            return fake_response()

        monkeypatch.setattr(requests.sessions.Session, "request", fake_request)
        install_requests_protection()
        requests.get("https://quotes.sina.cn/cn/api/x")
        assert calls[0]["headers"].get("User-Agent")
        assert "sina.com.cn" in calls[0]["headers"].get("Referer", "")

    def test_no_inject_on_non_sina_url(self, monkeypatch):
        calls = []

        def fake_request(self, method, url, *args, **kwargs):
            calls.append(kwargs)
            return fake_response()

        monkeypatch.setattr(requests.sessions.Session, "request", fake_request)
        install_requests_protection()
        requests.get("https://datacenter-web.eastmoney.com/api/data/v1/get")
        assert (
            calls[0].get("headers") is None or "User-Agent" not in calls[0]["headers"]
        )

    def test_preserves_explicit_headers(self, monkeypatch):
        calls = []

        def fake_request(self, method, url, *args, **kwargs):
            calls.append(kwargs)
            return fake_response()

        monkeypatch.setattr(requests.sessions.Session, "request", fake_request)
        install_requests_protection()
        requests.get(
            "https://quotes.sina.cn/cn/api/x",
            headers={"User-Agent": "custom-agent"},
        )
        assert calls[0]["headers"]["User-Agent"] == "custom-agent"

    def test_456_raises_sina_blocked(self, monkeypatch):
        monkeypatch.setattr(
            requests.sessions.Session,
            "request",
            lambda self, method, url, *a, **kw: fake_response(status_code=456),
        )
        install_requests_protection()
        with pytest.raises(SinaBlockedError, match="456"):
            requests.get("https://quotes.sina.cn/cn/api/x")

    def test_html_block_page_raises(self, monkeypatch):
        html = "<html><title>拒绝访问</title>已被新浪安全部门封禁</html>".encode("gbk")
        monkeypatch.setattr(
            requests.sessions.Session,
            "request",
            lambda self, method, url, *a, **kw: fake_response(
                status_code=200, content_type="text/html", content=html
            ),
        )
        install_requests_protection()
        with pytest.raises(SinaBlockedError, match="200"):
            requests.get("https://quotes.sina.cn/cn/api/x")

    def test_blocked_not_raised_for_non_sina(self, monkeypatch):
        monkeypatch.setattr(
            requests.sessions.Session,
            "request",
            lambda self, method, url, *a, **kw: fake_response(status_code=456),
        )
        install_requests_protection()
        resp = requests.get("https://datacenter-web.eastmoney.com/api/data/v1/get")
        assert resp.status_code == 456

    def test_cooldown_after_50_sina_requests(self, monkeypatch):
        import utils.requests_protection as rp

        rp._sina_request_count = 0
        sleeps = []
        monkeypatch.setattr(rp.time, "sleep", lambda d: sleeps.append(d))
        monkeypatch.setattr(
            requests.sessions.Session,
            "request",
            lambda self, method, url, *a, **kw: fake_response(),
        )
        install_requests_protection()
        for _ in range(50):
            requests.get("https://quotes.sina.cn/cn/api/x")
        assert sleeps and sleeps[-1] == rp._COOL_DOWN_SECONDS

    def test_install_is_idempotent(self, monkeypatch):
        import utils.requests_protection as rp

        calls = []

        def fake_request(self, method, url, *args, **kwargs):
            calls.append(1)
            return fake_response()

        monkeypatch.setattr(requests.sessions.Session, "request", fake_request)
        install_requests_protection()
        install_requests_protection()
        install_requests_protection()
        requests.get("https://quotes.sina.cn/cn/api/x")
        assert len(calls) == 1
        assert rp._installed is True
