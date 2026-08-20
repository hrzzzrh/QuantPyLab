"""单元测试: utils/sina_klc.py SinaKlcFetcher (mock 网络与 JS 引擎, 不触真实网络)"""

import pandas as pd
import pytest

from utils.sina_klc import SinaHfqFetchError, SinaKlcFetcher, SinaKlcFetchError

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

    hfq_json = {"total": 1, "data": [{"d": "2026-08-03", "f": "2.0"}]}

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


def test_fetch_hfq_raises_on_request_error(monkeypatch):
    """请求异常 → 抛出复权获取异常，禁止静默降级"""
    import requests

    def raise_error(*args, **kwargs):
        raise ConnectionError("network down")

    monkeypatch.setattr(requests, "get", raise_error)
    with pytest.raises(SinaHfqFetchError, match="获取新浪复权因子失败"):
        SinaKlcFetcher.fetch_hfq("sh600519")


def test_fetch_hfq_raises_on_malformed_payload(monkeypatch):
    """缺少因子数据 → 抛出复权获取异常"""
    import requests

    class FakeResp:
        status_code = 200
        text = "var sh600519hfq={'total':1, 'data':[]}"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    with pytest.raises(SinaHfqFetchError, match="获取新浪复权因子失败"):
        SinaKlcFetcher.fetch_hfq("sh600519")


def test_fetch_hfq_raises_on_http_error(monkeypatch):
    """非 200 响应 → 抛出复权获取异常"""
    import requests

    class FakeResp:
        status_code = 503
        text = "temporary unavailable"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    with pytest.raises(SinaHfqFetchError, match="HTTP 503"):
        SinaKlcFetcher.fetch_hfq("sh600519")


def test_fetch_hfq_raises_sina_blocked_on_http_456(monkeypatch):
    import requests

    from utils.requests_protection import SinaBlockedError

    class FakeResp:
        status_code = 456
        text = "blocked"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    with pytest.raises(SinaBlockedError, match="HTTP 456"):
        SinaKlcFetcher.fetch_hfq("sh600519")


def test_fetch_raw_sets_request_timeout(monkeypatch):
    import py_mini_racer
    import requests

    captured = {}

    class FakeResp:
        status_code = 200
        text = "var KLC_K2_sh600519=FAKE;"

    def fake_get(*args, **kwargs):
        captured.update(kwargs)
        return FakeResp()

    class FakeJS:
        def eval(self, code):
            pass

        def call(self, fn_name, raw_str):
            return []

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())
    assert SinaKlcFetcher.fetch_raw("sh600519") == []
    assert captured["timeout"] == 30


def test_fetch_raw_raises_on_http_error(monkeypatch):
    import requests

    class FakeResp:
        status_code = 503
        text = "temporary unavailable"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    with pytest.raises(SinaKlcFetchError, match="sh600519.*HTTP 503"):
        SinaKlcFetcher.fetch_raw("sh600519")


def test_fetch_raw_wraps_request_error(monkeypatch):
    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("timed out")),
    )
    with pytest.raises(SinaKlcFetchError, match="timed out"):
        SinaKlcFetcher.fetch_raw("sh600519")


def test_fetch_raw_wraps_decode_error(monkeypatch):
    import py_mini_racer
    import requests

    class FakeResp:
        status_code = 200
        text = "malformed"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())

    class FakeJS:
        def eval(self, _code):
            raise ValueError("malformed")

    monkeypatch.setattr(py_mini_racer, "MiniRacer", lambda: FakeJS())
    with pytest.raises(SinaKlcFetchError, match="malformed"):
        SinaKlcFetcher.fetch_raw("sh600519")


def test_fetch_raw_raises_sina_blocked_on_http_456(monkeypatch):
    import requests

    from utils.requests_protection import SinaBlockedError

    class FakeResp:
        status_code = 456
        text = "blocked"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    with pytest.raises(SinaBlockedError, match="HTTP 456"):
        SinaKlcFetcher.fetch_raw("sh600519")


@pytest.mark.parametrize(
    "hfq_json",
    [
        {"total": False, "data": []},
        {"total": 0},
        {"total": 0, "data": [{"d": "2026-08-03", "f": "2.0"}]},
        {"total": 2, "data": [{"d": "2026-08-03", "f": "2.0"}]},
        {"total": 1, "data": [{"d": "2026-08-03"}]},
        {"total": 1, "data": [{"d": "2026-08-03", "f": "nan"}]},
        {"total": 1, "data": [{"d": "not-a-date", "f": "2.0"}]},
    ],
)
def test_fetch_hfq_rejects_invalid_payload(monkeypatch, hfq_json):
    """非法 total、行数或字段 → 抛出复权获取异常"""
    import requests

    class FakeResp:
        status_code = 200
        text = f"var sh600519hfq={repr(hfq_json)}"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    with pytest.raises(SinaHfqFetchError, match="获取新浪复权因子失败"):
        SinaKlcFetcher.fetch_hfq("sh600519")


def test_fetch_hfq_propagates_sina_blocked(monkeypatch):
    """新浪风控不可降级为缺失复权因子, 必须原样传播"""
    import requests

    from utils.requests_protection import SinaBlockedError

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(SinaBlockedError("IP 风控测试")),
    )
    with pytest.raises(SinaBlockedError, match="IP 风控测试"):
        SinaKlcFetcher.fetch_hfq("sh600519")


# ──────────────────── fetch_klc_data ────────────────────


def test_fetch_klc_data_merges_hfq(monkeypatch):
    """raw + hfq 合并 → 标准列 + adj_factor"""
    import requests

    js_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 10.0,
            "high": 10.2,
            "low": 10.0,
            "close": 10.1,
            "volume": 1_000_000,
            "amount": 1_010_000,
            "prevclose": 10.0,
            "postVol": 0.0,
            "postAmt": 0.0,
        },
        {
            "date": "2026-08-04T00:00:00.000Z",
            "open": 10.1,
            "high": 10.3,
            "low": 10.0,
            "close": 10.2,
            "volume": 800_000,
            "amount": 816_000,
            "prevclose": 10.1,
            "postVol": 0.0,
            "postAmt": 0.0,
            "future_metric": 123.0,
        },
    ]
    hfq_factor = 0.123456789012346
    hfq_json = {
        "total": 1,
        "data": [{"d": "2026-08-03", "f": str(hfq_factor)}],
    }

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
    assert df["close_hfq"].iloc[0] == pytest.approx(10.1 * hfq_factor)
    assert df["adj_factor"].iloc[0] == hfq_factor
    assert df["adj_factor"].iloc[1] == hfq_factor
    assert df.attrs["hfq_source_rows"] == 1
    assert df.attrs["hfq_forward_filled_rows"] == 1


def test_fetch_klc_data_wraps_non_numeric_raw_fields(monkeypatch):
    raw_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": "not-a-number",
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "volume": 1000,
            "amount": 10000,
        }
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )

    with pytest.raises(SinaKlcFetchError, match="非数值字段"):
        SinaKlcFetcher.fetch_klc_data("sh600519")


@pytest.mark.parametrize("invalid_value", [None, float("nan"), float("inf")])
def test_fetch_klc_data_rejects_non_finite_raw_fields(monkeypatch, invalid_value):
    raw_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": invalid_value,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "volume": 1000,
            "amount": 10000,
        }
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )

    with pytest.raises(SinaKlcFetchError, match="非有限数值"):
        SinaKlcFetcher.fetch_klc_data("sh600519")


def test_fetch_klc_data_rejects_hfq_gap_before_raw_start(monkeypatch):
    raw_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "volume": 1000,
            "amount": 10000,
        }
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_hfq",
        staticmethod(
            lambda _symbol: pd.DataFrame({"date": ["2026-08-04"], "hfq_factor": [2.0]})
        ),
    )

    with pytest.raises(SinaHfqFetchError, match="未覆盖 raw 起始日期"):
        SinaKlcFetcher.fetch_klc_data("sh600519")


def test_fetch_klc_data_uses_window_initial_hfq_factor(monkeypatch):
    raw_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "volume": 1000,
            "amount": 10000,
        },
        {
            "date": "2026-08-04T00:00:00.000Z",
            "open": 11.0,
            "high": 12.0,
            "low": 10.0,
            "close": 11.0,
            "volume": 1000,
            "amount": 11000,
        },
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_hfq",
        staticmethod(
            lambda _symbol: pd.DataFrame({"date": ["2026-08-01"], "hfq_factor": [2.0]})
        ),
    )

    frame = SinaKlcFetcher.fetch_klc_data("sh600519", "20260803", "20260804")
    assert frame["adj_factor"].tolist() == [2.0, 2.0]
    assert frame.attrs["hfq_source_rows"] == 1
    assert frame.attrs["hfq_forward_filled_rows"] == 2


def test_fetch_hfq_rejects_duplicate_dates(monkeypatch):
    import requests

    payload = {
        "total": 2,
        "data": [
            {"d": "2026-08-03", "f": "2.0"},
            {"d": "2026-08-03", "f": "2.1"},
        ],
    }

    class FakeResp:
        status_code = 200
        text = f"var x={repr(payload)}"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResp())
    with pytest.raises(SinaHfqFetchError, match="重复日期"):
        SinaKlcFetcher.fetch_hfq("sh600519")


def test_fetch_klc_data_propagates_hfq_fetch_error(monkeypatch):
    """复权请求失败 → 不得将整段数据降级为 adj_factor=1.0"""
    raw_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "volume": 1000,
            "amount": 10000,
        }
    ]

    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_raw",
        staticmethod(lambda sina_symbol: raw_data),
    )

    def raise_hfq_error(sina_symbol):
        raise SinaHfqFetchError(f"测试复权请求失败: {sina_symbol}")

    monkeypatch.setattr(SinaKlcFetcher, "fetch_hfq", staticmethod(raise_hfq_error))

    with pytest.raises(SinaHfqFetchError, match="测试复权请求失败"):
        SinaKlcFetcher.fetch_klc_data("sh600519")


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
    frame = SinaKlcFetcher.fetch_klc_data("sh600519")
    assert frame.empty
    assert list(frame.columns) == [
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
    assert (df["adj_factor"] == 1.0).all()


def test_fetch_klc_data_clamps_start_date_to_minimum(monkeypatch):
    raw_data = [
        {
            "date": "2009-12-31T00:00:00.000Z",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
            "amount": 150,
        },
        {
            "date": "2010-01-04T00:00:00.000Z",
            "open": 2.0,
            "high": 3.0,
            "low": 1.5,
            "close": 2.5,
            "volume": 200,
            "amount": 500,
        },
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_hfq",
        staticmethod(lambda _symbol: None),
    )

    frame = SinaKlcFetcher.fetch_klc_data(
        "sh600519", start_date="19900101", end_date="20100104"
    )

    assert frame["date"].tolist() == ["2010-01-04"]


def test_fetch_klc_data_filters_known_bad_row(monkeypatch):
    raw_data = [
        {
            "date": "2024-11-06T00:00:00.000Z",
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 20.92,
            "volume": 100,
            "amount": 150,
        },
        {
            "date": "2024-11-07T00:00:00.000Z",
            "open": 21.0,
            "high": 22.0,
            "low": 20.0,
            "close": 21.5,
            "volume": 200,
            "amount": 500,
        },
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_hfq",
        staticmethod(lambda _symbol: None),
    )

    frame = SinaKlcFetcher.fetch_klc_data("sh688089")

    assert frame["date"].tolist() == ["2024-11-07"]
    assert frame.attrs["known_bad_rows_filtered"] == 1


def test_fetch_klc_data_skips_hfq_when_window_is_empty(monkeypatch):
    raw_data = [
        {
            "date": "2026-08-01T00:00:00.000Z",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
            "amount": 150,
        }
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )

    def fail_if_called(_symbol):
        raise AssertionError("空日期窗口不应请求 hfq")

    monkeypatch.setattr(SinaKlcFetcher, "fetch_hfq", staticmethod(fail_if_called))

    frame = SinaKlcFetcher.fetch_klc_data("sh600519", "20260803", "20260805")
    assert frame.empty
    assert list(frame.columns) == [
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


def test_fetch_klc_data_rejects_invalid_raw_date_before_filtering(monkeypatch):
    raw_data = [
        {
            "date": "2026-08-03T00:00:00.000Z",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
            "amount": 150,
        },
        {
            "date": "not-a-date",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
            "amount": 150,
        },
    ]
    monkeypatch.setattr(
        SinaKlcFetcher, "fetch_raw", staticmethod(lambda _symbol: raw_data)
    )

    with pytest.raises(SinaKlcFetchError, match="无效日期"):
        SinaKlcFetcher.fetch_klc_data("sh600519", "20260803", "20260803")


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
    with pytest.raises(SinaKlcFetchError, match="缺少 date"):
        SinaKlcFetcher.fetch_list_date("000508")


def test_fetch_list_date_rejects_invalid_date_after_first_record(monkeypatch):
    monkeypatch.setattr(
        SinaKlcFetcher,
        "fetch_raw",
        staticmethod(
            lambda _symbol: [
                {"date": "1993-04-30T00:00:00.000Z", "close": 1.0},
                {"date": "not-a-date", "close": 1.1},
            ]
        ),
    )

    with pytest.raises(SinaKlcFetchError, match="无效日期"):
        SinaKlcFetcher.fetch_list_date("000508")
