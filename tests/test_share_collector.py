"""单元测试: ShareCollector 风控异常传播与重试边界。"""

import pytest

from data_ingestion.collectors.share_collector import ShareCollector
from utils.requests_protection import SinaBlockedError


def test_collect_share_capital_does_not_retry_sina_blocked(monkeypatch):
    """新浪风控命中 fatal_exceptions 后公共入口只调用一次"""
    collector = ShareCollector()
    calls = []
    monkeypatch.setattr(collector, "_get_local_max_date", lambda symbol: "1990-01-01")

    def blocked(*args, **kwargs):
        calls.append(1)
        raise SinaBlockedError("IP 风控测试")

    monkeypatch.setattr(collector, "_fetch_sina_share_capital", blocked)
    with pytest.raises(SinaBlockedError, match="IP 风控测试"):
        collector.collect_share_capital("600519")
    assert calls == [1]
