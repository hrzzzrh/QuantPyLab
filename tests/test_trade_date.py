"""单元测试: utils/trade_date.py 交易日历（mock 网络与缓存，不触真实网络）"""

from datetime import date, datetime

import pandas as pd
import pytest

import utils.trade_date as trade_date_mod
from utils.trade_date import _get_all_trade_dates, get_latest_trade_date

# 2026-08-03(周一) ~ 2026-08-07(周五)
MOCK_TRADE_DATES = [
    date(2026, 8, 3),
    date(2026, 8, 4),
    date(2026, 8, 5),
    date(2026, 8, 6),
    date(2026, 8, 7),
]


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path, monkeypatch):
    """每个测试使用独立的缓存文件并清空内存缓存"""
    monkeypatch.setattr(
        trade_date_mod, "CACHE_FILE", tmp_path / "trade_calendar.parquet"
    )
    _get_all_trade_dates.cache_clear()
    yield
    _get_all_trade_dates.cache_clear()


def _mock_ak_fetch(monkeypatch, trade_dates=None, raises=None):
    def fake_fetch():
        if raises:
            raise raises
        return pd.DataFrame({"trade_date": trade_dates or MOCK_TRADE_DATES})

    monkeypatch.setattr(trade_date_mod.ak, "tool_trade_date_hist_sina", fake_fetch)


class TestGetLatestTradeDate:
    def test_weekend_returns_previous_friday(self, monkeypatch, tmp_path):
        """周六调用应返回上周五"""
        _mock_ak_fetch(monkeypatch)
        ref = datetime(2026, 8, 8, 10, 0)
        assert get_latest_trade_date(ref) == date(2026, 8, 7)

    def test_before_close_returns_previous_day(self, monkeypatch):
        """交易日 15:30 前调用应返回上一交易日"""
        _mock_ak_fetch(monkeypatch)
        ref = datetime(2026, 8, 7, 14, 0)
        assert get_latest_trade_date(ref) == date(2026, 8, 6)

    def test_at_close_time_edge(self, monkeypatch):
        """15:30:00 整点应视为已收盘，返回当天"""
        _mock_ak_fetch(monkeypatch)
        ref = datetime(2026, 8, 7, 15, 30)
        assert get_latest_trade_date(ref) == date(2026, 8, 7)

    def test_after_close_returns_same_day(self, monkeypatch):
        _mock_ak_fetch(monkeypatch)
        ref = datetime(2026, 8, 7, 16, 0)
        assert get_latest_trade_date(ref) == date(2026, 8, 7)

    def test_holiday_returns_last_trade_day(self, monkeypatch):
        """非交易日（周末以外假期）应回退到最近交易日"""
        _mock_ak_fetch(monkeypatch)
        ref = datetime(2026, 8, 6, 9, 0)  # 周四未开盘时间
        assert get_latest_trade_date(ref) == date(2026, 8, 5)


class TestTradeCalendarCache:
    def test_fetches_and_persists_cache(self, monkeypatch, tmp_path):
        """无缓存时从网络同步并持久化"""
        _mock_ak_fetch(monkeypatch)
        dates = _get_all_trade_dates()
        assert dates == MOCK_TRADE_DATES
        assert (tmp_path / "trade_calendar.parquet").exists()

    def test_fresh_cache_skips_network(self, monkeypatch, tmp_path):
        """缓存足够新时不再请求网络"""
        cache_path = tmp_path / "trade_calendar.parquet"
        pd.DataFrame({"trade_date": MOCK_TRADE_DATES}).to_parquet(
            cache_path, index=False
        )
        _mock_ak_fetch(monkeypatch, raises=RuntimeError("network down"))
        assert _get_all_trade_dates() == MOCK_TRADE_DATES

    def test_stale_cache_fallback(self, monkeypatch, tmp_path):
        """网络失败但存在旧缓存时应兜底返回旧缓存"""
        stale_dates = [date(2025, 2, 7)]
        pd.DataFrame({"trade_date": stale_dates}).to_parquet(
            tmp_path / "trade_calendar.parquet", index=False
        )
        _mock_ak_fetch(monkeypatch, raises=RuntimeError("network down"))
        assert _get_all_trade_dates() == stale_dates

    def test_stale_cache_triggers_refresh(self, monkeypatch, tmp_path):
        """缓存过期时应触发网络同步"""
        pd.DataFrame({"trade_date": [date(2025, 2, 7)]}).to_parquet(
            tmp_path / "trade_calendar.parquet", index=False
        )
        _mock_ak_fetch(monkeypatch)
        assert _get_all_trade_dates() == MOCK_TRADE_DATES

    def test_no_cache_and_network_fail_raises(self, monkeypatch):
        """无缓存且网络失败时应抛出异常（不静默返回错误数据）"""
        _mock_ak_fetch(monkeypatch, raises=RuntimeError("network down"))
        with pytest.raises(RuntimeError):
            _get_all_trade_dates()
