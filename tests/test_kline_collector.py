"""单元测试: DailyKlineCollector — Sina klc 解码 / 日级冷却 / CDR fallback"""

from datetime import date

import pandas as pd
import pytest

from data_ingestion.collectors.kline_collector import DailyKlineCollector
from storage.database import manager as manager_mod
from storage.database.sync_status import (
    DATASET_KLINE_DAILY,
    is_synced_today,
    record_sync_success,
)


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

    hfq_json = {"total": 2, "data": [{"d": "2026-08-03", "f": "2.0"}]}
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

    yesterday = (
        date.today().replace(day=date.today().day - 1)
        if date.today().day > 1
        else date.today()
    )
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
    monkeypatch.setattr(collector, "_save_incremental", lambda df, sym: None)
    result = collector.collect_kline("600519")
    assert result is True
    assert fetched[0] is True


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
