"""Unit tests for Tencent newfqkline raw/hfq parsing."""

from datetime import date, timedelta

import pandas as pd
import pytest

from utils.tencent_kline import (
    TencentKlineFetcher,
    TencentKlineFetchError,
    TencentKlineTransientError,
)


def make_row(
    row_date: str,
    *,
    close: float = 10.0,
    volume: float = 100.0,
    amount_wan: float = 20.0,
) -> list[object]:
    return [
        row_date,
        f"{close - 0.1:.2f}",
        f"{close:.2f}",
        f"{close + 0.2:.2f}",
        f"{close - 0.2:.2f}",
        f"{volume:.2f}",
        {},
        "0.50",
        f"{amount_wan:.2f}",
        "0.00",
        "0.00",
    ]


def make_frame(close_hfq: tuple[float, float]) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = [date(2026, 8, 3), date(2026, 8, 4)]
    raw = pd.DataFrame(
        {
            "date": dates,
            "open": [9.9, 10.9],
            "high": [10.2, 11.2],
            "low": [9.8, 10.8],
            "close": [10.0, 11.0],
            "volume": [100.0, 200.0],
            "amount": [200_000.0, 440_000.0],
        }
    )
    hfq = pd.DataFrame({"date": dates, "close_hfq": close_hfq})
    return raw, hfq


def test_fetch_page_parses_newfq_day_and_amount(monkeypatch):
    import requests

    captured = {}
    rows = [make_row("2026-08-12", amount_wan=471761.31)]

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "code": 0,
                "msg": "",
                "data": {"sz000003": {"day": rows}},
            }

    def fake_get(url, **kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(requests, "get", fake_get)

    result = TencentKlineFetcher.fetch_page("sz000003", "2026-08-12", "")

    assert result == rows
    assert captured["params"]["param"] == ("sz000003,day,2010-01-01,2026-08-12,640,")


def test_fetch_page_classifies_transport_failure(monkeypatch):
    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(requests.Timeout("timed out")),
    )

    with pytest.raises(TencentKlineTransientError, match="暂时失败"):
        TencentKlineFetcher.fetch_page("sh600009", "2026-08-07", "")


@pytest.mark.parametrize("status_code", [408, 425, 429, 500, 503])
def test_fetch_page_classifies_retryable_http_status(monkeypatch, status_code):
    import requests

    class FakeResponse:
        pass

    FakeResponse.status_code = status_code

    def raise_for_status(self):
        raise requests.HTTPError("retryable status", response=self)

    FakeResponse.raise_for_status = raise_for_status
    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: FakeResponse())

    with pytest.raises(TencentKlineTransientError, match=f"status={status_code}"):
        TencentKlineFetcher.fetch_page("sh600009", "2026-08-07", "")


def test_fetch_page_keeps_nonretryable_http_status_as_fetch_error(monkeypatch):
    import requests

    class FakeResponse:
        status_code = 404

        def raise_for_status(self):
            raise requests.HTTPError("not found", response=self)

    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: FakeResponse())

    with pytest.raises(TencentKlineFetchError) as exc_info:
        TencentKlineFetcher.fetch_page("sh600009", "2026-08-07", "")
    assert type(exc_info.value) is TencentKlineFetchError


def test_fetch_page_classifies_generic_transport_exception(monkeypatch):
    import requests

    monkeypatch.setattr(
        requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            requests.RequestException("proxy unavailable")
        ),
    )

    with pytest.raises(TencentKlineTransientError, match="暂时失败"):
        TencentKlineFetcher.fetch_page("sh600009", "2026-08-07", "")


def test_fetch_series_converts_volume_and_amount_units(monkeypatch):
    row = make_row("2026-08-12", volume=123.0, amount_wan=45.67)
    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_page",
        staticmethod(lambda *_args: [row]),
    )

    result = TencentKlineFetcher.fetch_series(
        "sz000003", adjust="", start_date="20260812", end_date="20260812"
    )

    assert result["volume"].tolist() == [123.0]
    assert result["amount"].tolist() == pytest.approx([456_700.0])


@pytest.mark.parametrize("data", [None, [], "invalid"])
def test_fetch_page_rejects_invalid_data_container(monkeypatch, data):
    import requests

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"code": 0, "data": data}

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(TencentKlineFetchError, match="data"):
        TencentKlineFetcher.fetch_page("sz000003", "2026-08-12", "")


def test_fetch_series_moves_pagination_end_before_previous_page(monkeypatch):
    calls = []
    first_page_dates = [
        (date(2024, 1, 1) + timedelta(days=index)).isoformat() for index in range(640)
    ]
    second_page_dates = [
        (date(2022, 1, 1) + timedelta(days=index)).isoformat() for index in range(639)
    ]
    pages = [
        [make_row(row_date) for row_date in first_page_dates],
        [make_row(row_date) for row_date in second_page_dates],
    ]

    def fake_page(symbol, end_date, adjust):
        calls.append((symbol, end_date, adjust))
        return pages[len(calls) - 1]

    monkeypatch.setattr(TencentKlineFetcher, "fetch_page", staticmethod(fake_page))

    result = TencentKlineFetcher.fetch_series(
        "sz000003",
        adjust="",
        start_date="20220101",
        end_date="20260812",
    )

    assert calls == [
        ("sz000003", "2026-08-12", ""),
        ("sz000003", "2023-12-31", ""),
    ]
    assert len(result) == 1279
    assert result["date"].is_monotonic_increasing


@pytest.mark.parametrize("bad_row", [["2026-08-12"], ["not-a-date", 1, 2, 3, 4, 5]])
def test_fetch_series_rejects_malformed_pagination_row(monkeypatch, bad_row):
    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_page",
        staticmethod(lambda *_args: [bad_row]),
    )

    with pytest.raises(TencentKlineFetchError):
        TencentKlineFetcher.fetch_series(
            "sz000003", adjust="", start_date="20260812", end_date="20260812"
        )


def test_fetch_full_converts_amount_and_calculates_factor(monkeypatch):
    raw, hfq = make_frame((20.0, 33.0))

    def fake_series(symbol, *, adjust, start_date, end_date):
        return raw if adjust == "" else hfq

    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_series",
        staticmethod(fake_series),
    )

    result = TencentKlineFetcher.fetch_full("000003")

    assert list(result.columns) == [
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
    assert result["amount"].tolist() == [200_000.0, 440_000.0]
    assert result["adj_factor"].tolist() == pytest.approx([2.0, 3.0])
    assert result.attrs["source"] == "tencent-newfq"
    assert result.attrs["amount_source"] == "tencent-newfq"


def test_fetch_full_filters_weekends_and_records_count(monkeypatch):
    dates = [date(2026, 8, 8), date(2026, 8, 10)]
    raw, hfq = make_frame((20.0, 33.0))
    raw["date"] = dates
    hfq["date"] = dates

    def fake_series(symbol, *, adjust, start_date, end_date):
        return raw if adjust == "" else hfq

    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_series",
        staticmethod(fake_series),
    )

    result = TencentKlineFetcher.fetch_full("000003")

    assert result["date"].tolist() == [date(2026, 8, 10)]
    assert result["adj_factor"].tolist() == pytest.approx([3.0])
    assert result.attrs["weekend_rows_filtered"] == 1
    assert result.attrs["hfq_source_rows"] == 2


def test_fetch_full_filters_known_bad_row_and_records_count(monkeypatch):
    raw, hfq = make_frame((20.0, 33.0))
    dates = [date(2024, 11, 6), date(2024, 11, 7)]
    raw["date"] = dates
    hfq["date"] = dates

    def fake_series(symbol, *, adjust, start_date, end_date):
        return raw if adjust == "" else hfq

    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_series",
        staticmethod(fake_series),
    )

    result = TencentKlineFetcher.fetch_full("688089")

    assert result["date"].tolist() == [date(2024, 11, 7)]
    assert result.attrs["known_bad_rows_filtered"] == 1


def test_fetch_full_compares_raw_hfq_after_weekend_filter(monkeypatch):
    raw, hfq = make_frame((20.0, 33.0))
    raw["date"] = [date(2026, 8, 8), date(2026, 8, 10)]
    hfq["date"] = [date(2026, 8, 9), date(2026, 8, 10)]

    def fake_series(symbol, *, adjust, start_date, end_date):
        return raw if adjust == "" else hfq

    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_series",
        staticmethod(fake_series),
    )

    result = TencentKlineFetcher.fetch_full("000003")

    assert result["date"].tolist() == [date(2026, 8, 10)]
    assert result.attrs["weekend_rows_filtered"] == 2


def test_fetch_series_clamps_start_date_to_minimum(monkeypatch):
    row = make_row("2010-01-04")
    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_page",
        staticmethod(lambda *_args: [row]),
    )

    result = TencentKlineFetcher.fetch_series(
        "sz000003",
        adjust="",
        start_date="19900101",
        end_date="20100104",
    )

    assert result["date"].tolist() == [date(2010, 1, 4)]


def test_fetch_full_wraps_quality_failure_with_source(monkeypatch):
    raw, hfq = make_frame((20.0, 33.0))
    raw.loc[0, "high"] = 9.0

    def fake_series(symbol, *, adjust, start_date, end_date):
        return raw if adjust == "" else hfq

    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_series",
        staticmethod(fake_series),
    )

    with pytest.raises(TencentKlineFetchError, match="质量校验") as exc_info:
        TencentKlineFetcher.fetch_full("000003")

    assert exc_info.value.source_used == "tencent-newfq"


def test_fetch_full_rejects_raw_hfq_date_mismatch(monkeypatch):
    raw, hfq = make_frame((20.0, 33.0))
    hfq.loc[1, "date"] = date(2026, 8, 5)

    def fake_series(symbol, *, adjust, start_date, end_date):
        return raw if adjust == "" else hfq

    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_series",
        staticmethod(fake_series),
    )

    with pytest.raises(TencentKlineFetchError, match="日期集合不一致"):
        TencentKlineFetcher.fetch_full("000003")


def test_fetch_full_marks_empty_result_source(monkeypatch):
    monkeypatch.setattr(
        TencentKlineFetcher,
        "fetch_series",
        staticmethod(lambda *args, **kwargs: pd.DataFrame()),
    )

    result = TencentKlineFetcher.fetch_full("000003")

    assert result.empty
    assert result.attrs["source"] == "tencent-newfq"
    assert result.attrs["amount_unit"] == "yuan"


def test_fetch_page_rejects_missing_amount(monkeypatch):
    import requests

    invalid_row = make_row("2026-08-12")
    invalid_row[8] = ""

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"code": 0, "data": {"sz000003": {"day": [invalid_row]}}}

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResponse())
    with pytest.raises(TencentKlineFetchError, match="amount"):
        TencentKlineFetcher.fetch_series(
            "sz000003", adjust="", start_date="20260812", end_date="20260812"
        )
