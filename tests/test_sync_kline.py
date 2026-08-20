"""单元测试: sync_daily_kline 增量跳过与限速节奏 (mock 采集器, 不触网络)"""

from contextlib import nullcontext

import main as main_mod


class _FakePbar:
    """模拟 tqdm 迭代器: 可迭代且带 set_description"""

    def __init__(self, items):
        self._items = items

    def __iter__(self):
        return iter(self._items)

    def set_description(self, desc):
        pass


class _FakeLatestTradeDate:
    def strftime(self, fmt):
        return "20260807"


def _mock_env(monkeypatch, stock_codes, collect_results):
    """构造 sync_daily_kline 运行环境, 返回 (sleeps, fetch_calls)"""
    import data_ingestion.collectors.kline_collector as kc_mod
    import utils.trade_date as td_mod

    sleeps = []
    fetch_calls = []

    class FakeCollector:
        def __init__(self, source=None):
            self.source = source

        def collect_kline(self, code, start_date=None, end_date=None):
            fetch_calls.append((code, start_date, end_date))
            return collect_results.get(code, False)

    monkeypatch.setattr(main_mod.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(main_mod, "tqdm", lambda items, **kw: _FakePbar(items))
    monkeypatch.setattr(main_mod, "CanonicalWriteLock", lambda **kwargs: nullcontext())
    monkeypatch.setattr(
        main_mod,
        "get_all_stocks",
        lambda: [(c, f"name_{c}") for c in stock_codes],
    )
    # 函数内 import: 需 patch 源模块
    monkeypatch.setattr(td_mod, "get_latest_trade_date", lambda: _FakeLatestTradeDate())
    monkeypatch.setattr(kc_mod, "DailyKlineCollector", FakeCollector)
    return sleeps, fetch_calls


def test_sync_daily_kline_uses_sina_klc_source(monkeypatch):
    import data_ingestion.collectors.kline_collector as kc_mod
    import utils.trade_date as td_mod

    sources = []

    class FakeCollector:
        def __init__(self, source=None):
            sources.append(source)

        def collect_kline(self, *args, **kwargs):
            return False

    monkeypatch.setattr(main_mod, "CanonicalWriteLock", lambda **kwargs: nullcontext())
    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(main_mod, "tqdm", lambda items, **kwargs: _FakePbar(items))
    monkeypatch.setattr(td_mod, "get_latest_trade_date", lambda: _FakeLatestTradeDate())
    monkeypatch.setattr(kc_mod, "DailyKlineCollector", FakeCollector)

    main_mod.sync_daily_kline()

    assert sources == ["sina-klc"]


def test_up_to_date_stocks_skip_fetch_and_sleep(monkeypatch):
    """已是最新 (collect_kline 返回 False) 的股票: 不 sleep, 计入跳过"""
    sleeps, fetch_calls = _mock_env(
        monkeypatch,
        stock_codes=["600519", "000001"],
        collect_results={"600519": False, "000001": False},
    )
    main_mod.sync_daily_kline()

    assert fetch_calls == [
        ("600519", None, "20260807"),
        ("000001", None, "20260807"),
    ]
    assert sleeps == [], "已是最新的股票不应触发限速 sleep"


def test_fetching_stocks_sleep_after_sync(monkeypatch):
    """实际抓取 (返回 True) 的股票才 sleep, 且每次抓取后恰好一次"""
    sleeps, fetch_calls = _mock_env(
        monkeypatch,
        stock_codes=["600519", "000001"],
        collect_results={"600519": True, "000001": False},
    )
    main_mod.sync_daily_kline()

    assert len(sleeps) == 1, f"仅实际抓取的股票应 sleep, 实际 sleep {len(sleeps)} 次"


def test_failed_stock_does_not_sleep(monkeypatch):
    """抓取抛异常: 记录失败不 sleep, 不中断后续股票"""
    import data_ingestion.collectors.kline_collector as kc_mod

    sleeps, fetch_calls = _mock_env(
        monkeypatch,
        stock_codes=["600519", "000001"],
        collect_results={"000001": False},
    )

    class BoomCollector:
        def __init__(self, source=None):
            self.source = source

        def collect_kline(self, code, start_date=None, end_date=None):
            fetch_calls.append((code, start_date, end_date))
            if code == "600519":
                raise RuntimeError("模拟失败")
            return False

    monkeypatch.setattr(kc_mod, "DailyKlineCollector", BoomCollector)
    main_mod.sync_daily_kline()

    assert len(fetch_calls) == 2, "失败不应中断后续股票"
    assert sleeps == [], "失败股票不应 sleep"


def test_delisted_empty_data_counts_as_failure(monkeypatch):
    import data_ingestion.collectors.kline_collector as kc_mod
    from data_ingestion.collectors.kline_collector import KlineDataUnavailableError

    sleeps, fetch_calls = _mock_env(
        monkeypatch,
        stock_codes=["600421"],
        collect_results={},
    )

    class EmptyDelistedCollector:
        def __init__(self, source=None):
            self.source = source

        def collect_kline(self, code, start_date=None, end_date=None):
            fetch_calls.append((code, start_date, end_date))
            raise KlineDataUnavailableError("无行情数据")

    monkeypatch.setattr(kc_mod, "DailyKlineCollector", EmptyDelistedCollector)
    processed, failed = main_mod.sync_daily_kline()

    assert (processed, failed) == (1, 1)
    assert len(fetch_calls) == 1
    assert sleeps == []


def test_explicit_start_date_passed_through(monkeypatch):
    """显式传入 start_date 时原样透传给 collector"""
    sleeps, fetch_calls = _mock_env(
        monkeypatch,
        stock_codes=["600519"],
        collect_results={"600519": False},
    )
    main_mod.sync_daily_kline(start_date="20260101")

    assert fetch_calls[0][1] == "20260101"
    assert fetch_calls[0][2] == "20260807"


def test_force_all_uses_kline_minimum_start_date(monkeypatch):
    _, fetch_calls = _mock_env(
        monkeypatch,
        stock_codes=["600519"],
        collect_results={"600519": False},
    )

    main_mod.sync_daily_kline(force_all=True)

    assert fetch_calls == [("600519", "20100101", "20260807")]
