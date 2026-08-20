import pandas as pd
import pytest

from utils.kline_policy import drop_known_bad_kline_rows, normalize_kline_start_date


def test_normalize_kline_start_date_uses_configured_minimum():
    assert normalize_kline_start_date(None) == "20100101"
    assert normalize_kline_start_date("19900101") == "20100101"
    assert normalize_kline_start_date("20100101") == "20100101"
    assert normalize_kline_start_date("20260101") == "20260101"


def test_normalize_kline_start_date_rejects_invalid_date():
    with pytest.raises(ValueError, match="YYYYMMDD"):
        normalize_kline_start_date("2010-01-01")


def test_drop_known_bad_kline_rows_filters_only_configured_symbol_date():
    frame = pd.DataFrame(
        {
            "date": ["2024-11-06", "2024-11-07"],
            "close": [20.92, 21.0],
        }
    )

    filtered, excluded_dates = drop_known_bad_kline_rows(frame, "688089")

    assert filtered["date"].tolist() == ["2024-11-07"]
    assert excluded_dates == {"2024-11-06"}
    assert filtered.attrs["known_bad_rows_filtered"] == 1


def test_drop_known_bad_kline_rows_leaves_other_symbols_unchanged():
    frame = pd.DataFrame({"date": ["2024-11-06"], "close": [20.92]})

    filtered, excluded_dates = drop_known_bad_kline_rows(frame, "688090")

    assert len(filtered) == 1
    assert excluded_dates == set()
    assert filtered.attrs["known_bad_rows_filtered"] == 0
