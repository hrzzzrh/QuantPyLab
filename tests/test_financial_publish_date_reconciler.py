"""测试四源财务公告日期统一口径。"""

from pathlib import Path

import pandas as pd

import storage.database.financial_publish_date_reconciler as reconciler_mod
import storage.database.financial_store as financial_store_mod
import storage.database.indicator_store as indicator_store_mod
import storage.file_store.parquet_store as parquet_store_mod
from storage.database.financial_publish_date_reconciler import (
    DATA_AVAILABLE_DATE_COLUMN,
    build_canonical_publish_date_map,
    build_data_available_date_map,
    normalize_financial_dates,
    reconcile_financial_publish_dates_for_symbol,
)

SYMBOL = "000418"
SOURCE_CATEGORIES = reconciler_mod.FINANCIAL_SOURCE_CATEGORIES


def _write_source(
    warehouse: Path,
    source_name: str,
    dates: list[str],
    values: list[object],
) -> None:
    frame = pd.DataFrame(
        {
            "report_date": dates,
            "公告日期": values,
            "value": range(len(dates)),
        }
    )
    path = (
        warehouse / SOURCE_CATEGORIES[source_name] / f"symbol={SYMBOL}" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_normalize_financial_dates():
    series = pd.Series(["2016-10-25 00:00:00", "20151027", "20241301", None, "invalid"])

    result = normalize_financial_dates(series)

    assert result.astype("string").tolist() == [
        "20161025",
        "20151027",
        pd.NA,
        pd.NA,
        pd.NA,
    ]


def test_build_canonical_publish_date_map_uses_earliest_non_null_date():
    source_frames = {
        "balance": pd.DataFrame(
            {
                "report_date": ["20150930", "20160930"],
                "公告日期": ["20151027", "20161025"],
            }
        ),
        "income": pd.DataFrame(
            {
                "report_date": ["20150930", "20160930"],
                "公告日期": ["20161025", "20171025"],
            }
        ),
        "cashflow": pd.DataFrame(
            {
                "report_date": ["20150930", "20160930"],
                "公告日期": ["20161025", "20171025"],
            }
        ),
        "indicator": pd.DataFrame(
            {
                "report_date": ["20150930", "20160930"],
                "公告日期": ["2016-10-25 00:00:00", "2016-10-25 00:00:00"],
            }
        ),
    }

    result = build_canonical_publish_date_map(source_frames)

    assert result.to_dict() == {"20150930": "20151027", "20160930": "20161025"}
    assert build_data_available_date_map(source_frames).to_dict() == {
        "20150930": "20161025",
        "20160930": "20171025",
    }


def test_reconcile_overwrites_all_available_sources(tmp_path):
    _write_source(
        tmp_path, "balance", ["20150930", "20160930"], ["20151027", "20161025"]
    )
    _write_source(
        tmp_path, "income", ["20150930", "20160930"], ["20161025", "20171025"]
    )
    _write_source(
        tmp_path, "cashflow", ["20150930", "20160930"], ["20161025", "20171025"]
    )
    _write_source(
        tmp_path,
        "indicator",
        ["20150930", "20160930"],
        ["2016-10-25 00:00:00", "2016-10-25 00:00:00"],
    )

    changed = reconcile_financial_publish_dates_for_symbol(SYMBOL, tmp_path)

    assert changed == {
        "balance": 2,
        "income": 2,
        "cashflow": 2,
        "indicator": 2,
    }
    expected = ["20151027", "20161025"]
    expected_available = ["20161025", "20171025"]
    for source_name, category in SOURCE_CATEGORIES.items():
        path = tmp_path / category / f"symbol={SYMBOL}" / "data.parquet"
        result = pd.read_parquet(path)
        assert result["公告日期"].tolist() == expected
        assert result[DATA_AVAILABLE_DATE_COLUMN].tolist() == expected_available
        assert result["value"].tolist() == [0, 1]


def test_financial_store_triggers_date_reconciliation(tmp_path, monkeypatch):
    monkeypatch.setattr(financial_store_mod, "WAREHOUSE_DIR", tmp_path)
    monkeypatch.setattr(parquet_store_mod, "WAREHOUSE_DIR", tmp_path)
    monkeypatch.setattr(
        financial_store_mod.db_manager,
        "get_duckdb_conn",
        lambda: object(),
    )
    calls = []

    def fake_reconcile(symbol):
        calls.append(symbol)
        return {"balance": 1}

    monkeypatch.setattr(
        financial_store_mod,
        "reconcile_financial_publish_dates_for_symbol",
        fake_reconcile,
    )

    changed = financial_store_mod.FinancialStore().save_statement(
        pd.DataFrame(
            {
                "symbol": [SYMBOL],
                "report_date": ["20150930"],
                "公告日期": ["20151027"],
                "营业总收入": [100.0],
            }
        ),
        "fin_balance_sheet",
    )

    assert calls == [SYMBOL]
    assert changed == {"balance": 1}


def test_indicator_store_triggers_date_reconciliation(tmp_path, monkeypatch):
    monkeypatch.setattr(indicator_store_mod, "WAREHOUSE_DIR", tmp_path)
    monkeypatch.setattr(parquet_store_mod, "WAREHOUSE_DIR", tmp_path)
    monkeypatch.setattr(
        indicator_store_mod.db_manager,
        "get_duckdb_conn",
        lambda: object(),
    )
    calls = []

    def fake_reconcile(symbol):
        calls.append(symbol)
        return {"indicator": 1}

    monkeypatch.setattr(
        indicator_store_mod,
        "reconcile_financial_publish_dates_for_symbol",
        fake_reconcile,
    )

    changed = indicator_store_mod.IndicatorStore().save_indicators(
        pd.DataFrame(
            {
                "symbol": [SYMBOL],
                "report_date": ["20150930"],
                "公告日期": ["2016-10-25 00:00:00"],
                "归属净利润": [100.0],
            }
        )
    )

    assert calls == [SYMBOL]
    assert changed == {"indicator": 1}
