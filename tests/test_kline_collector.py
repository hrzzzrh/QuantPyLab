"""单元测试: DailyKlineCollector — Sina klc 解码 / 日级冷却 / CDR fallback"""

from datetime import date, timedelta

import pandas as pd
import pytest

from data_ingestion.collectors.kline_collector import (
    DailyKlineCollector,
    KlineDataTransientError,
)
from storage.database import manager as manager_mod
from storage.database.sync_status import (
    DATASET_KLINE_DAILY,
    DATASET_KLINE_DAILY_NO_DATA,
    get_last_sync_date,
    is_synced_today,
    record_sync_success,
)
from utils.kline_validation import KlineValidationError


@pytest.fixture(autouse=True)
def _isolate_env(tmp_path, monkeypatch):
    from storage.file_store import parquet_store as parquet_store_mod

    sqlite_path = tmp_path / "test_metadata.db"
    warehouse_dir = tmp_path / "warehouse"
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    monkeypatch.setattr(manager_mod, "WAREHOUSE_DIR", str(warehouse_dir))
    monkeypatch.setattr(parquet_store_mod, "WAREHOUSE_DIR", str(warehouse_dir))
    from storage.database import sync_status as ss

    monkeypatch.setattr(ss.db_manager, "sqlite_path", sqlite_path)
    ss.db_manager._sqlite_conn = None
    ss.db_manager.initialize_schema()
    conn = ss.db_manager.get_sqlite_conn()
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES ('600519', '600519', '贵州茅台', 1)"
    )
    conn.commit()
    yield
    ss.db_manager._sqlite_conn = None


# ──────────────────── _fetch_sina_klc ────────────────────


def _make_fake_js(js_data):
    """构建 Fake MiniRacer, 返回指定的 JS 解密数据"""

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return js_data

    return FakeJS


def test_fetch_sina_klc_decodes_and_merges_hfq(monkeypatch):
    """验证 klc_kl.js JS 解密 + hfq 合并 + adj_factor 计算"""
    collector = DailyKlineCollector()

    js_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 1600.0,
            "high": 1650.0,
            "low": 1590.0,
            "close": 1640.0,
            "volume": 1_000_000,
            "amount": 164_000_000,
            "prevclose": 1620.0,
            "postVol": 0.0,
            "postAmt": 0.0,
        },
        {
            "date": "2026-08-04T00:00:00.000Z",
            "open": 1610.0,
            "high": 1620.0,
            "low": 1605.0,
            "close": 1615.0,
            "volume": 800_000,
            "amount": 129_200_000,
            "prevclose": 1640.0,
            "postVol": 0.0,
            "postAmt": 0.0,
        },
    ]

    import py_mini_racer

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: _make_fake_js(js_data)())

    # Mock requests.get: klc_kl.js 由 internal patch 处理, hfq.js 由 global side_effect 处理
    import requests

    hfq_json = {"total": 1, "data": [{"d": "2026-08-03", "f": "2.0"}]}
    orig_get = requests.get

    def side_effect_get(*args, **kwargs):
        url = args[0] if args else kwargs.get("url", "")
        if "hfq.js" in url:
            resp = type("Resp", (), {})()
            resp.status_code = 200
            resp.text = f"var sh600519hfq={repr(hfq_json)}"
            return resp
        if "hisdata_klc2" in url:
            resp = type("Resp", (), {})()
            resp.status_code = 200
            resp.text = "var KLC_K2_sh600519=FAKE;"
            return resp
        return orig_get(*args, **kwargs)

    monkeypatch.setattr(requests, "get", side_effect_get)

    df = collector._fetch_sina_klc(
        "sh600519", start_date="20200101", end_date="20261231"
    )
    assert len(df) == 2
    assert df["close_hfq"].iloc[0] == pytest.approx(3280.0)
    assert df["adj_factor"].iloc[0] == pytest.approx(2.0)
    assert df["adj_factor"].iloc[1] == pytest.approx(2.0)


def test_fetch_sina_klc_empty_hfq(monkeypatch):
    """hfq.js 返回 total=0 时 adj_factor 恒为 1.0"""
    import requests

    collector = DailyKlineCollector()

    mock_raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-08-03"]),
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [500_000],
            "amount": [5_250_000],
            "prevclose": [10.0],
            "postVol": [0.0],
            "postAmt": [0.0],
        }
    )

    import py_mini_racer

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return mock_raw.to_dict(orient="records")

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())

    class FakeKlcResp:
        status_code = 200
        text = "var KLC_K2=FAKE"

    class FakeHfqResp:
        status_code = 200
        text = "var sh600519hfq={'total':0, 'data':[]}"

    def side_effect_get(url, **kwargs):
        if "hfq.js" in url:
            return FakeHfqResp()
        return FakeKlcResp()

    monkeypatch.setattr(requests, "get", side_effect_get)

    df = collector._fetch_sina_klc("sh600519")
    assert len(df) == 1
    assert df["adj_factor"].iloc[0] == 1.0
    assert df["close_hfq"].iloc[0] == 10.5


def test_fetch_from_sina_rejects_partial_hfq_coverage(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    raw = pd.DataFrame(
        {
            "date": ["2026-08-03", "2026-08-04"],
            "open": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.5, 10.5],
            "close": [10.2, 11.2],
            "volume": [1000.0, 1100.0],
            "amount": [10000.0, 12000.0],
        }
    )
    hfq = pd.DataFrame({"date": ["2026-08-03"], "close": [20.4]})

    def fake_daily(**kwargs):
        return raw if kwargs["adjust"] == "" else hfq

    monkeypatch.setattr(kline_mod.ak, "stock_zh_a_daily", fake_daily)

    with pytest.raises(KlineValidationError, match="未覆盖"):
        collector._fetch_from_sina("600519", "20260801", "20260807")


def test_fetch_from_em_rejects_empty_hfq(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    raw = pd.DataFrame(
        {
            "日期": ["2026-08-03"],
            "开盘": [10.0],
            "最高": [10.5],
            "最低": [9.5],
            "收盘": [10.2],
            "成交量": [1000.0],
            "成交额": [10000.0],
        }
    )
    hfq = pd.DataFrame(columns=["日期", "收盘"])

    def fake_hist(**kwargs):
        return raw if kwargs["adjust"] == "" else hfq

    monkeypatch.setattr(kline_mod.ak, "stock_zh_a_hist", fake_hist)

    with pytest.raises(KlineValidationError, match="后复权数据为空"):
        collector._fetch_from_em("600519", "20260801", "20260807")


def test_collect_kline_propagates_hfq_fetch_error_for_active_stock(monkeypatch):
    """在市股票复权失败仍应重试后向上抛出"""
    import data_ingestion.collectors.kline_collector as kline_mod
    from utils import retry as retry_mod
    from utils.sina_klc import SinaHfqFetchError

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda symbol: False)
    monkeypatch.setattr(
        kline_mod,
        "get_latest_trade_date",
        lambda: type("TradeDate", (), {"strftime": lambda self, fmt: "20260807"})(),
    )
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: None)

    def raise_hfq_error(*args, **kwargs):
        raise SinaHfqFetchError("复权接口失败")

    monkeypatch.setattr(collector, "_fetch_from_sina", raise_hfq_error)

    with pytest.raises(SinaHfqFetchError, match="复权接口失败"):
        collector.collect_kline("600519", start_date="20260803", end_date="20260807")


def test_collect_kline_retries_transient_delisted_source_failure(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod
    from utils import retry as retry_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda _symbol: True)
    monkeypatch.setattr(retry_mod.time, "sleep", lambda _seconds: None)
    calls = []
    frame = pd.DataFrame(
        {
            "date": ["2026-08-07"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.5],
            "close": [10.2],
            "volume": [100.0],
            "amount": [1000.0],
            "adj_factor": [1.0],
        }
    )

    def rebuild(_symbol):
        calls.append(1)
        if len(calls) < 3:
            raise KlineDataTransientError("腾讯暂时不可用")
        return frame

    monkeypatch.setattr(collector, "_fetch_delisted_rebuild_frame", rebuild)
    monkeypatch.setattr(kline_mod, "get_last_sync_date", lambda *_args: None)
    monkeypatch.setattr(kline_mod, "is_synced_today", lambda *_args: False)

    assert collector.collect_kline("600519") is True
    assert calls == [1, 1, 1]


def test_fetch_sina_klc_empty_data(monkeypatch):
    """JS 解密返回空列表时返回空 DataFrame"""
    import requests

    collector = DailyKlineCollector()

    import py_mini_racer

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return []

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())

    class FakeResp:
        status_code = 200
        text = "var KLC_K2=FAKE"

    monkeypatch.setattr(requests, "get", lambda url, **kwargs: FakeResp())
    df = collector._fetch_sina_klc("sh600519")
    assert df.empty


# ──────────────────── daily cooldown (DATASET_KLINE_DAILY) ────────────────────


def test_collect_kline_skips_on_daily_cooldown(monkeypatch):
    """当日已同步 (DATASET_KLINE_DAILY) 且无新数据时直接跳过"""
    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(
        collector,
        "_get_local_max_date",
        lambda s: "20260731",
    )

    record_sync_success(DATASET_KLINE_DAILY, "600519", date.today())
    assert is_synced_today(DATASET_KLINE_DAILY, "600519")

    fetch_called = [False]
    monkeypatch.setattr(
        collector,
        "_fetch_from_sina",
        lambda s, sd, ed: fetch_called.__setitem__(0, True),
    )
    result = collector.collect_kline("600519")
    assert result is False
    assert fetch_called[0] is False


def test_collect_kline_retries_after_cooldown_expires(monkeypatch):
    """冷却到期后 (次日) 重新抓取"""
    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(collector, "_get_local_max_date", lambda s: "20260731")

    yesterday = date.today() - timedelta(days=1)
    record_sync_success(DATASET_KLINE_DAILY, "600519", yesterday)
    assert not is_synced_today(DATASET_KLINE_DAILY, "600519")

    fetched = [False]

    def fake_fetch(sym, sd, ed):
        fetched[0] = True
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-08-07", "2026-08-07"]),
                "open": [1610, 1610],
                "high": [1620, 1620],
                "low": [1605, 1605],
                "close": [1615, 1615],
                "volume": [800.0, 800.0],
                "amount": [129_200_000, 129_200_000],
                "close_hfq": [1615, 1615],
                "adj_factor": [1.0, 1.0],
            }
        )

    monkeypatch.setattr(collector, "_fetch_from_sina", fake_fetch)
    result = collector.collect_kline("600519")
    assert result is True
    assert fetched[0] is True
    assert is_synced_today(DATASET_KLINE_DAILY, "600519")


def test_collect_kline_rebuilds_status_for_current_local_partition(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(collector, "_get_local_max_date", lambda s: "20260812")
    monkeypatch.setattr(kline_mod, "get_latest_trade_date", lambda: date(2026, 8, 12))
    collector.store.save_partition(
        pd.DataFrame(
            {
                "date": ["2026-08-12"],
                "open": [10.0],
                "high": [10.5],
                "low": [9.5],
                "close": [10.2],
                "volume": [100.0],
                "amount": [1000.0],
                "adj_factor": [1.0],
            }
        ),
        "daily_kline",
        "600519",
    )
    record_sync_success(DATASET_KLINE_DAILY_NO_DATA, "600519", date.today())
    fetch_called = []
    monkeypatch.setattr(
        collector, "_fetch_from_sina", lambda *args: fetch_called.append(1)
    )

    result = collector.collect_kline("600519", end_date="20260812")

    assert result is False
    assert fetch_called == []
    assert is_synced_today(DATASET_KLINE_DAILY, "600519")
    assert get_last_sync_date(DATASET_KLINE_DAILY_NO_DATA, "600519") is None


def test_collect_kline_records_no_data_without_success_status(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(collector, "_get_local_max_date", lambda s: "20260731")
    monkeypatch.setattr(kline_mod, "get_latest_trade_date", lambda: date(2026, 8, 12))
    monkeypatch.setattr(collector, "_fetch_from_sina", lambda *args: pd.DataFrame())

    result = collector.collect_kline("600519", end_date="20260812")

    assert result is False
    assert get_last_sync_date(DATASET_KLINE_DAILY, "600519") is None
    assert get_last_sync_date(DATASET_KLINE_DAILY_NO_DATA, "600519") == date.today()


def test_explicit_start_date_bypasses_no_data_cooldown(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(kline_mod, "get_latest_trade_date", lambda: date(2026, 8, 12))
    record_sync_success(DATASET_KLINE_DAILY_NO_DATA, "600519", date.today())
    fetched = []
    monkeypatch.setattr(
        collector,
        "_fetch_from_sina",
        lambda *args: fetched.append(args) or pd.DataFrame(),
    )

    result = collector.collect_kline(
        "600519", start_date="20260801", end_date="20260812"
    )

    assert result is False
    assert len(fetched) == 1


def test_collect_kline_repairs_missing_status_after_save(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod
    from utils import retry as retry_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(kline_mod, "get_latest_trade_date", lambda: date(2026, 8, 12))
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: None)
    fetch_calls = []
    monkeypatch.setattr(
        collector,
        "_fetch_from_sina",
        lambda *args: (
            fetch_calls.append(1)
            or pd.DataFrame(
                {
                    "date": ["2026-08-12"],
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.5],
                    "close": [10.2],
                    "volume": [100.0],
                    "amount": [1000.0],
                    "adj_factor": [1.0],
                }
            )
        ),
    )

    original_record = kline_mod.record_sync_success
    attempts = []

    def fail_once(dataset, symbol, sync_date):
        attempts.append(dataset)
        if len(attempts) == 1:
            raise RuntimeError("metadata unavailable")
        return original_record(dataset, symbol, sync_date)

    monkeypatch.setattr(kline_mod, "record_sync_success", fail_once)

    result = collector.collect_kline("600519", end_date="20260812")

    assert result is True
    assert attempts == [DATASET_KLINE_DAILY, DATASET_KLINE_DAILY]
    assert fetch_calls == [1]
    assert get_last_sync_date(DATASET_KLINE_DAILY, "600519") == date.today()


def test_collect_kline_retries_no_data_status_without_refetching(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod
    from utils import retry as retry_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(kline_mod, "get_latest_trade_date", lambda: date(2026, 8, 12))
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: None)
    fetch_calls = []
    monkeypatch.setattr(
        collector,
        "_fetch_from_sina",
        lambda *args: fetch_calls.append(1) or pd.DataFrame(),
    )
    original_record = kline_mod.record_sync_success
    attempts = []

    def fail_no_data_once(dataset, symbol, sync_date):
        if dataset == DATASET_KLINE_DAILY_NO_DATA:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("metadata unavailable")
        return original_record(dataset, symbol, sync_date)

    monkeypatch.setattr(kline_mod, "record_sync_success", fail_no_data_once)

    result = collector.collect_kline("600519", end_date="20260812")

    assert result is False
    assert fetch_calls == [1]
    assert attempts == [1, 1]


def test_collect_kline_does_not_record_status_when_save_fails(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod
    from utils import retry as retry_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(kline_mod, "get_latest_trade_date", lambda: date(2026, 8, 12))
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        collector,
        "_fetch_from_sina",
        lambda *args: pd.DataFrame(
            {
                "date": ["2026-08-12"],
                "open": [10.0],
                "high": [10.5],
                "low": [9.5],
                "close": [10.2],
                "volume": [100.0],
                "amount": [1000.0],
                "adj_factor": [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        collector,
        "_save_incremental",
        lambda *args: (_ for _ in ()).throw(RuntimeError("disk unavailable")),
    )

    with pytest.raises(RuntimeError, match="disk unavailable"):
        collector.collect_kline("600519", end_date="20260812")

    assert get_last_sync_date(DATASET_KLINE_DAILY, "600519") is None


def test_collect_kline_rejects_invalid_klc_fallback_before_save(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod
    from utils import retry as retry_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda s: False)
    monkeypatch.setattr(kline_mod, "get_latest_trade_date", lambda: date(2026, 8, 12))
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: None)
    invalid = pd.DataFrame(
        {
            "date": ["2026-08-12"],
            "open": [10.0],
            "high": [10.5],
            "low": [10.2],
            "close": [10.3],
            "volume": [100.0],
            "amount": [1000.0],
            "close_hfq": [10.3],
            "adj_factor": [1.0],
        }
    )
    invalid.attrs["source"] = "sina-klc"
    monkeypatch.setattr(collector, "_fetch_from_sina", lambda *args: invalid)
    saved = []
    monkeypatch.setattr(collector, "_save_incremental", lambda *args: saved.append(1))

    with pytest.raises(ValueError, match="开盘价"):
        collector.collect_kline("600519", end_date="20260812")

    assert saved == []


def test_collect_kline_does_not_retry_sina_blocked(monkeypatch):
    """新浪风控命中 fatal_exceptions 后公共入口只调用一次"""
    import data_ingestion.collectors.kline_collector as kline_mod
    from utils.requests_protection import SinaBlockedError

    collector = DailyKlineCollector()
    calls = []
    monkeypatch.setattr(collector, "_is_delisted", lambda symbol: False)
    monkeypatch.setattr(
        kline_mod,
        "get_latest_trade_date",
        lambda: type("TradeDate", (), {"strftime": lambda self, fmt: "20260807"})(),
    )
    monkeypatch.setattr(collector, "_get_local_max_date", lambda symbol: "19900101")
    monkeypatch.setattr(kline_mod, "is_synced_today", lambda *args: False)

    def blocked(*args, **kwargs):
        calls.append(1)
        raise SinaBlockedError("IP 风控测试")

    monkeypatch.setattr(collector, "_fetch_from_sina", blocked)
    with pytest.raises(SinaBlockedError, match="IP 风控测试"):
        collector.collect_kline("600519", end_date="20260807")
    assert calls == [1]


def test_collect_kline_clamps_pre_cutoff_start_date(monkeypatch):
    collector = DailyKlineCollector()
    calls = []
    monkeypatch.setattr(collector, "_is_delisted", lambda symbol: False)
    monkeypatch.setattr(
        collector,
        "_fetch_from_sina",
        lambda *args: calls.append(args) or pd.DataFrame(),
    )

    assert (
        collector.collect_kline("600519", start_date="19900101", end_date="20260807")
        is False
    )
    assert calls == [("600519", "20100101", "20260807")]


def test_collect_kline_filters_pre_cutoff_rows_from_any_source(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_is_delisted", lambda symbol: False)
    captured = []
    monkeypatch.setattr(
        collector,
        "_fetch_from_sina",
        lambda *args: pd.DataFrame(
            {
                "date": ["2009-12-31", "2024-11-06", "2024-11-07"],
                "open": [1.0, 0.0, 2.0],
                "high": [1.1, 0.0, 2.1],
                "low": [0.9, 0.0, 1.9],
                "close": [1.0, 20.92, 2.0],
                "volume": [100.0, 100.0, 200.0],
                "amount": [1000.0, 1000.0, 2000.0],
                "close_hfq": [1.0, 20.92, 2.0],
                "adj_factor": [1.0, 1.0, 1.0],
            }
        ),
    )
    monkeypatch.setattr(
        collector,
        "_save_incremental",
        lambda frame, symbol: captured.append(frame.copy()),
    )

    assert (
        collector.collect_kline("688089", start_date="19900101", end_date="20241107")
        is True
    )
    assert captured[0]["date"].astype(str).tolist() == ["2024-11-07"]


def test_save_incremental_removes_known_bad_rows_from_existing_partition():
    collector = DailyKlineCollector()
    old = pd.DataFrame(
        {
            "date": ["2024-11-06", "2024-11-07"],
            "open": [0.0, 2.0],
            "high": [0.0, 2.1],
            "low": [0.0, 1.9],
            "close": [20.92, 2.0],
            "volume": [100.0, 200.0],
            "amount": [1000.0, 2000.0],
            "adj_factor": [1.0, 1.0],
        }
    )
    collector.store.save_partition(old, "daily_kline", "688089")

    collector._save_incremental(
        pd.DataFrame(
            {
                "date": ["2024-11-08"],
                "open": [2.0],
                "high": [2.1],
                "low": [1.9],
                "close": [2.0],
                "volume": [200.0],
                "amount": [2000.0],
                "adj_factor": [1.0],
            }
        ),
        "688089",
    )

    path = collector.store.base_dir / "daily_kline" / "symbol=688089" / "data.parquet"
    result = pd.read_parquet(path)
    assert result["date"].astype(str).tolist() == ["2024-11-07", "2024-11-08"]


def test_save_incremental_holds_lock_across_read_and_write(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    old = pd.DataFrame(
        {
            "date": ["2024-11-07"],
            "open": [2.0],
            "high": [2.1],
            "low": [1.9],
            "close": [2.0],
            "volume": [200.0],
            "amount": [2000.0],
            "adj_factor": [1.0],
        }
    )
    collector.store.save_partition(old, "daily_kline", "600519")

    events = []

    class FakeLock:
        def __enter__(self):
            events.append("enter")

        def __exit__(self, exc_type, exc_value, traceback):
            events.append("exit")

    original_read_parquet = kline_mod.pd.read_parquet

    def record_read(path):
        events.append("read")
        return original_read_parquet(path)

    monkeypatch.setattr(kline_mod, "canonical_write_lock_held", lambda _path: False)
    monkeypatch.setattr(
        kline_mod, "CanonicalWriteLock", lambda *args, **kwargs: FakeLock()
    )
    monkeypatch.setattr(kline_mod.pd, "read_parquet", record_read)
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda *args, **kwargs: events.append("save"),
    )

    collector._save_incremental(
        old.assign(date=["2024-11-08"]),
        "600519",
    )

    assert events == ["enter", "read", "save", "exit"]


# ──────────────────── CDR fallback ────────────────────


def test_fetch_from_sina_falls_back_to_cdr_on_error(monkeypatch):
    """akshare 解析异常时切换 _fetch_sina_klc 备用路径"""
    collector = DailyKlineCollector()

    fallback_called = [False]
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: (
            fallback_called.__setitem__(0, True) or pd.DataFrame()
        ),
    )

    import akshare as ak

    def fake_daily(*args, **kwargs):
        raise ValueError("模拟 akshare 解析失败")

    monkeypatch.setattr(ak, "stock_zh_a_daily", fake_daily)

    # Mock requests.get 及其 patch
    import requests

    class FakeResp:
        status_code = 200
        text = ""

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())

    collector._fetch_from_sina("689009", "20260801", "20260807")
    assert fallback_called[0] is True


def test_fetch_from_sina_sina_blocked_propagates_without_cdr(monkeypatch):
    """IP 风控时立即传播 SinaBlockedError, 不降级 CDR (同域接口, 降级放大封禁)"""
    from utils.requests_protection import SinaBlockedError

    collector = DailyKlineCollector()

    cdr_called = []
    monkeypatch.setattr(
        collector,
        "_fetch_cdr_sina",
        lambda *args, **kwargs: cdr_called.append(1) or pd.DataFrame(),
    )

    import akshare as ak

    def fake_daily(*args, **kwargs):
        raise SinaBlockedError("IP 风控测试")

    monkeypatch.setattr(ak, "stock_zh_a_daily", fake_daily)

    import requests

    class FakeResp:
        status_code = 200
        text = ""

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())

    with pytest.raises(SinaBlockedError, match="IP 风控测试"):
        collector._fetch_from_sina("689009", "20260801", "20260807")
    assert cdr_called == []


def test_fetch_from_sina_normal_path_not_affected(monkeypatch):
    """非异常情况正常走 akshare 路径"""
    collector = DailyKlineCollector()

    fallback_called = [False]
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: (
            fallback_called.__setitem__(0, True) or pd.DataFrame()
        ),
    )

    import akshare as ak

    # akshare 返回的 DataFrame: date 是列, 不是 index
    df_raw = pd.DataFrame(
        {
            "date": [date(2026, 8, 7)],
            "open": [1610.0],
            "high": [1620.0],
            "low": [1605.0],
            "close": [1615.0],
            "volume": [800_000.0],
            "amount": [129_200_000.0],
            "outstanding_share": [1_250_000.0],
            "turnover": [0.01],
        }
    )

    df_hfq = pd.DataFrame(
        {
            "date": [date(2026, 8, 7)],
            "open": [1610.0],
            "high": [1620.0],
            "low": [1605.0],
            "close": [3230.0],
            "volume": [800_000.0],
            "amount": [129_200_000.0],
            "outstanding_share": [1_250_000.0],
            "turnover": [0.01],
        }
    )

    def fake_daily(*args, adjust="", **kwargs):
        if adjust == "hfq":
            return df_hfq
        return df_raw

    monkeypatch.setattr(ak, "stock_zh_a_daily", fake_daily)

    import requests

    class FakeResp:
        status_code = 200
        text = ""

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())

    result = collector._fetch_from_sina("600519", "20260801", "20260807")
    assert not result.empty
    assert fallback_called[0] is False
    assert "adj_factor" in result.columns
