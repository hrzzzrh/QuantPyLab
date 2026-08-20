import pandas as pd
import pytest

import data_ingestion.collectors.industry_collector as collector_mod
import main as main_module
from data_ingestion.collectors.industry_collector import (
    INDUSTRY_HISTORY_COLUMNS,
    IndustryHistoryCollector,
    normalize_industry_history,
)


def _raw_frame():
    return pd.DataFrame(
        {
            "symbol": [1, "000002", "000001"],
            "start_date": ["1991-01-01", "2000-01-01", "2021-07-30"],
            "industry_code": [101, "430101", "480301"],
            "update_time": ["2015-10-27", "2015-10-27", "2025-12-15"],
        }
    )


def test_normalize_industry_history_preserves_codes_and_sorts_rows():
    normalized = normalize_industry_history(_raw_frame())

    assert normalized["symbol"].tolist() == ["000001", "000001", "000002"]
    assert normalized["industry_code"].tolist() == ["000101", "480301", "430101"]
    assert normalized["effective_date"].tolist() == [
        pd.Timestamp("1991-01-01").date(),
        pd.Timestamp("2021-07-30").date(),
        pd.Timestamp("2000-01-01").date(),
    ]
    assert list(normalized.columns) == [
        "symbol",
        *INDUSTRY_HISTORY_COLUMNS,
    ]


@pytest.mark.parametrize(
    "frame, message",
    [
        (pd.DataFrame(), "返回为空"),
        (_raw_frame().drop(columns=["industry_code"]), "缺少字段"),
        (
            _raw_frame().assign(symbol=["bad", "000002", "000001"]),
            "非法股票代码",
        ),
        (
            _raw_frame().assign(industry_code=["bad", "430101", "480301"]),
            "非法行业代码",
        ),
        (
            _raw_frame().assign(start_date=["not-a-date", "2000-01-01", "2021-07-30"]),
            "无法解析",
        ),
        (
            pd.concat([_raw_frame(), _raw_frame().iloc[[0]]], ignore_index=True),
            "重复",
        ),
    ],
)
def test_normalize_industry_history_rejects_invalid_data(frame, message):
    with pytest.raises(ValueError, match=message):
        normalize_industry_history(frame)


def test_collector_saves_one_partition_per_symbol_and_records_success(monkeypatch):
    snapshots = []
    statuses = []

    monkeypatch.setattr(collector_mod, "is_synced_today", lambda *args: False)
    monkeypatch.setattr(
        collector_mod,
        "save_snapshot_atomically",
        lambda base_dir, category, partitions, **kwargs: snapshots.append(
            (base_dir, category, partitions, kwargs)
        ),
    )
    monkeypatch.setattr(
        collector_mod,
        "record_sync_success",
        lambda *args: statuses.append(args),
    )

    collector = IndustryHistoryCollector(
        fetcher=lambda: _raw_frame(),
    )

    processed, failed = collector.sync(force_refresh=True)

    assert (processed, failed) == (3, 0)
    assert len(snapshots) == 1
    _base_dir, category, partitions, kwargs = snapshots[0]
    assert category == "industry_classification_sw"
    assert [item[1] for item in partitions] == ["000001", "000002"]
    assert kwargs == {
        "operation": "sync-industry-history",
        "run_id": "ALL",
    }
    assert all(
        list(item[0].columns) == list(INDUSTRY_HISTORY_COLUMNS) for item in partitions
    )
    assert statuses[0][0:2] == ("industry_history", "ALL")


def test_collector_skips_same_day_snapshot_before_fetching(monkeypatch):
    fetch_calls = []
    monkeypatch.setattr(collector_mod, "is_synced_today", lambda *args: True)

    collector = IndustryHistoryCollector(
        fetcher=lambda: fetch_calls.append(True),
    )

    assert collector.sync() == (0, 0)
    assert fetch_calls == []


def test_main_sync_industry_history_delegates_to_collector(monkeypatch):
    calls = []

    class FakeCollector:
        def sync(self, *, force_refresh):
            calls.append(force_refresh)
            return 12, 0

    monkeypatch.setattr(collector_mod, "IndustryHistoryCollector", FakeCollector)

    assert main_module.sync_industry_history(force_refresh=True) == (12, 0)
    assert calls == [True]
