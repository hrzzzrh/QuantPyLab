"""单元测试: utils/sina_klc.py SinaKlcFetcher (mock 网络与 JS 引擎, 不触真实网络)"""

import pytest

from utils.sina_klc import SinaKlcFetcher

# ──────────────────── fetch_raw ────────────────────


def test_fetch_raw_decodes_js(monkeypatch):
    """klc_kl.js 抓取 + JS 解密 → 返回原始 dict list"""
    import requests

    js_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 44.69,
            "high": 45.2,
            "low": 44.0,
            "close": 44.9,
            "volume": 9152553,
            "amount": 457984212,
        },
        {
            "date": "2026-08-04T00:00:00.000Z",
            "open": 43.17,
            "high": 44.0,
            "low": 42.8,
            "close": 43.5,
            "volume": 10235561,
            "amount": 445125736,
        },
    ]

    class FakeResp:
        status_code = 200
        text = "var KLC_K2_sh600519=FAKE;"

    def side_effect_get(*args, **kwargs):
        return FakeResp()

    monkeypatch.setattr(requests, "get", side_effect_get)

    import py_mini_racer

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return js_data

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())

    result = SinaKlcFetcher.fetch_raw("sh600519")
    assert len(result) == 2
    assert result[0]["date"] == "2026-08-03T00:00:00.000Z"
    assert result[0]["close"] == 44.9


def test_fetch_raw_empty_decodes_to_empty_list(monkeypatch):
    """JS 解密返回空时返回 []"""
    import requests

    class FakeResp:
        status_code = 200
        text = "var KLC_K2_sh600519=FAKE;"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())

    import py_mini_racer

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return None

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())

    assert SinaKlcFetcher.fetch_raw("sh600519") == []


# ──────────────────── fetch_hfq ────────────────────


def test_fetch_hfq_returns_factor_df(monkeypatch):
    """hfq.js 返回因子 → DataFrame (date, hfq_factor)"""
    import requests

    hfq_json = {"total": 2, "data": [{"d": "2026-08-03", "f": "2.0"}]}

    class FakeResp:
        status_code = 200
        text = f"var sh600519hfq={repr(hfq_json)}"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())

    df = SinaKlcFetcher.fetch_hfq("sh600519")
    assert df is not None
    assert list(df.columns) == ["date", "hfq_factor"]
    assert df["date"].iloc[0] == "2026-08-03"
    assert df["hfq_factor"].iloc[0] == 2.0


def test_fetch_hfq_none_when_no_factors(monkeypatch):
    """hfq.js 无因子 (total=0) → None"""
    import requests

    class FakeResp:
        status_code = 200
        text = "var sh600519hfq={'total':0, 'data':[]}"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    assert SinaKlcFetcher.fetch_hfq("sh600519") is None


def test_fetch_hfq_none_on_request_error(monkeypatch):
    """请求异常 → None (不抛出)"""
    import requests

    def raise_error(*args, **kwargs):
        raise ConnectionError("network down")

    monkeypatch.setattr(requests, "get", raise_error)
    assert SinaKlcFetcher.fetch_hfq("sh600519") is None


# ──────────────────── fetch_klc_data ────────────────────


def test_fetch_klc_data_merges_hfq(monkeypatch):
    """raw + hfq 合并 → 标准列 + adj_factor"""
    import requests

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
    hfq_json = {"total": 2, "data": [{"d": "2026-08-03", "f": "2.0"}]}

    def side_effect_get(url, **kwargs):
        if "hfq.js" in url:
            return type(
                "R", (), {"status_code": 200, "text": f"var x={repr(hfq_json)}"}
            )()
        return type(
            "R", (), {"status_code": 200, "text": "var KLC_K2_sh600519=FAKE;"}
        )()

    monkeypatch.setattr(requests, "get", side_effect_get)

    import py_mini_racer

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return js_data

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())

    df = SinaKlcFetcher.fetch_klc_data("sh600519")
    assert list(df.columns) == [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "close_hfq",
        "adj_factor",
    ]
    assert len(df) == 2
    assert df["close_hfq"].iloc[0] == pytest.approx(3280.0)
    assert df["adj_factor"].iloc[0] == pytest.approx(2.0)
    assert df["adj_factor"].iloc[1] == pytest.approx(2.0)


def test_fetch_klc_data_empty_raw(monkeypatch):
    """raw 为空 → 空 DataFrame"""
    import py_mini_racer

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return None

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())

    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: type(
            "R", (), {"status_code": 200, "text": "var x=FAKE;"}
        )(),
    )
    assert SinaKlcFetcher.fetch_klc_data("sh600519").empty


def test_fetch_klc_data_filters_by_date_range(monkeypatch):
    """start_date/end_date 过滤生效"""
    import requests

    js_data = [
        {
            "date": "2026-08-01T00:00:00.000Z",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
            "amount": 150,
        },
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 2.0,
            "high": 3.0,
            "low": 1.5,
            "close": 2.5,
            "volume": 200,
            "amount": 500,
        },
        {
            "date": "2026-08-05T00:00:00.000Z",
            "open": 3.0,
            "high": 4.0,
            "low": 2.5,
            "close": 3.5,
            "volume": 300,
            "amount": 1050,
        },
    ]

    class FakeResp:
        status_code = 200
        text = "var KLC_K2_sh600519=FAKE;"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())

    import py_mini_racer

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return js_data

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())

    # 无 hfq (total=0), 只验证日期过滤
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_hfq",
        staticmethod(lambda sina_symbol: None),
    )
    df = SinaKlcFetcher.fetch_klc_data("sh600519", "20260803", "20260805")
    assert df["date"].tolist() == ["2026-08-03", "2026-08-05"]


# ──────────────────── fetch_list_date ────────────────────


def test_fetch_list_date_returns_first_record_date(monkeypatch):
    """取 klc 首条日期 → YYYYMMDD"""
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_raw",
        staticmethod(
            lambda sina_symbol: [
                {"date": "1993-04-30T00:00:00.000Z", "close": 1.0},
                {"date": "1993-05-03T00:00:00.000Z", "close": 1.1},
            ]
        ),
    )
    assert SinaKlcFetcher.fetch_list_date("000508") == "19930430"


def test_fetch_list_date_none_when_empty(monkeypatch):
    """klc 无数据 → None"""
    monkeypatch.setattr(SinaKlcFetcher, "fetch_raw", staticmethod(lambda s: []))
    assert SinaKlcFetcher.fetch_list_date("000508") is None


def test_fetch_list_date_none_when_missing_date_key(monkeypatch):
    """记录缺 date 字段 → None"""
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_raw",
        staticmethod(lambda s: [{"close": 1.0}]),
    )
    assert SinaKlcFetcher.fetch_list_date("000508") is None
