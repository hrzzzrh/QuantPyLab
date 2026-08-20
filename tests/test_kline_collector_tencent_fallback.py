"""Unit tests for delisted-stock Sina-to-Tencent fallback routing."""

import pandas as pd
import pytest

from data_ingestion.collectors.kline_collector import (
    DailyKlineCollector,
    KlineDataTransientError,
    KlineDataUnavailableError,
)
from utils.sina_klc import SinaKlcFetchError
from utils.tencent_kline import TencentKlineTransientError


def make_tencent_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "date": ["2026-04-29"],
            "open": [4.08],
            "high": [4.1],
            "low": [4.0],
            "close": [4.05],
            "volume": [200.0],
            "amount": [810000.0],
            "close_hfq": [8.1],
            "adj_factor": [2.0],
        }
    )
    frame.attrs["source"] = "tencent-newfq"
    return frame


def test_delisted_fallback_does_not_call_tencent_when_sina_valid(monkeypatch):
    collector = DailyKlineCollector()
    valid = make_tencent_frame()
    valid.attrs["source"] = "sina-klc"
    monkeypatch.setattr(collector, "_fetch_sina_klc", lambda *args: valid)
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: (_ for _ in ()).throw(AssertionError("不应调用腾讯")),
    )

    result = collector._fetch_delisted_rebuild_frame("000003")

    assert result.attrs["source"] == "sina-klc"


def test_delisted_fallback_uses_tencent_after_sina_quality_failure(monkeypatch):
    collector = DailyKlineCollector()
    invalid = make_tencent_frame()
    invalid.loc[0, "low"] = 4.2
    tencent = make_tencent_frame()
    monkeypatch.setattr(collector, "_fetch_sina_klc", lambda *args: invalid)
    calls = []
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: calls.append(symbol) or tencent,
    )

    result = collector._fetch_delisted_rebuild_frame("000003")

    assert calls == ["000003"]
    assert result.attrs["source"] == "tencent-newfq"


def test_delisted_fallback_uses_tencent_after_sina_numeric_failure(monkeypatch):
    collector = DailyKlineCollector()
    tencent = make_tencent_frame()
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda *args: (_ for _ in ()).throw(SinaKlcFetchError("非数值字段")),
    )
    calls = []
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: calls.append(symbol) or tencent,
    )

    result = collector._fetch_delisted_rebuild_frame("000003")

    assert calls == ["000003"]
    assert result.attrs["source"] == "tencent-newfq"


def test_delisted_klc_filters_known_bad_row_before_validation(monkeypatch):
    collector = DailyKlineCollector()
    good = make_tencent_frame()
    bad = good.copy()
    bad["date"] = ["2024-11-06"]
    bad[["open", "high", "low"]] = 0.0
    bad["close"] = 20.92
    bad["close_hfq"] = 20.92
    frame = pd.concat([bad, good], ignore_index=True)
    frame.attrs["source"] = "sina-klc"
    monkeypatch.setattr(collector, "_fetch_sina_klc", lambda *args: frame)

    result = collector._fetch_delisted_rebuild_frame("688089")

    assert result["date"].astype(str).tolist() == ["2026-04-29"]


def test_delisted_tencent_fallback_filters_known_bad_row_before_validation(
    monkeypatch,
):
    collector = DailyKlineCollector()
    good = make_tencent_frame()
    bad = good.copy()
    bad["date"] = ["2024-11-06"]
    bad[["open", "high", "low"]] = 0.0
    bad["close"] = 20.92
    bad["close_hfq"] = 20.92
    frame = pd.concat([bad, good], ignore_index=True)
    frame.attrs["source"] = "tencent-newfq"
    monkeypatch.setattr(collector, "_fetch_sina_klc", lambda *args: pd.DataFrame())
    monkeypatch.setattr(collector, "_fetch_tencent_newfq", lambda _symbol: frame)

    result = collector._fetch_delisted_rebuild_frame("688089")

    assert result["date"].astype(str).tolist() == ["2026-04-29"]


def test_delisted_fallback_raises_when_both_sources_fail(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda *args: pd.DataFrame(),
    )
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: (_ for _ in ()).throw(KlineDataUnavailableError("腾讯不可用")),
    )

    with pytest.raises(KlineDataUnavailableError, match="腾讯不可用"):
        collector._fetch_delisted_rebuild_frame("000003")


def test_delisted_fallback_marks_tencent_transport_failure_transient(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(collector, "_fetch_sina_klc", lambda *args: pd.DataFrame())
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: (_ for _ in ()).throw(TencentKlineTransientError("腾讯超时")),
    )

    with pytest.raises(KlineDataTransientError, match="暂时不可用"):
        collector._fetch_delisted_rebuild_frame("000003")


def test_delisted_fallback_rejects_tencent_weekend_data(monkeypatch):
    collector = DailyKlineCollector()
    weekend = make_tencent_frame()
    weekend["date"] = ["2026-04-25"]
    monkeypatch.setattr(collector, "_fetch_sina_klc", lambda *args: pd.DataFrame())
    monkeypatch.setattr(collector, "_fetch_tencent_newfq", lambda symbol: weekend)

    with pytest.raises(KlineDataUnavailableError, match="周末"):
        collector._fetch_delisted_rebuild_frame("000003")
