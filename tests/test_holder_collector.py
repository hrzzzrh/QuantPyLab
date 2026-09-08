"""单元测试: HolderCollector 归一化、增量合并与同步契约 (mock, 不触网络与真实库)。"""

import pandas as pd
import pytest

import main as main_mod
from data_ingestion.collectors import holder_collector as holder_mod
from data_ingestion.collectors.holder_collector import (
    OUTPUT_COLUMNS,
    HolderCollector,
    _required_cninfo_quarters,
    normalize_cninfo_frame,
    normalize_holder_detail,
)


def _raw_frame():
    """模拟东财个股明细接口返回 (含截断代码、多余列、乱序日期)。"""
    return pd.DataFrame(
        {
            "股东户数统计截止日": ["2025-06-30", "2025-03-31", "2025-03-31"],
            "区间涨跌幅": [1.0, 2.0, 2.0],
            "股东户数-本次": [323841, 203606, 203700],
            "股东户数-上次": [203606, 199192, 199192],
            "股东户数-增减": [120235, 4414, 4508],
            "股东户数-增减比例": [59.05, 2.21, 2.26],
            "户均持股市值": [1.0, 2.0, 3.0],
            "股东户数公告日期": ["2025-08-30", "2025-04-26", "2025-04-26"],
            "代码": ["2594", "2594", "002594"],
            "名称": ["比亚迪", "比亚迪", "比亚迪"],
        }
    )


def test_normalize_pads_truncated_code_sorts_and_dedups():
    normalized = normalize_holder_detail(_raw_frame(), "002594")

    assert normalized["symbol"].tolist() == ["002594", "002594"]
    assert list(normalized.columns) == list(OUTPUT_COLUMNS)
    assert normalized["source"].tolist() == ["em", "em"]
    assert normalized["stat_date"].tolist() == [
        pd.Timestamp("2025-03-31").date(),
        pd.Timestamp("2025-06-30").date(),
    ]
    # 同一 stat_date 保留最后一条
    assert normalized["holder_count"].tolist() == [203700, 323841]


def test_normalize_drops_rows_missing_key_fields():
    frame = pd.DataFrame(
        {
            "股东户数统计截止日": ["2025-06-30", "2025-03-31", None],
            "股东户数-本次": [100, None, 300],
            "股东户数-上次": [90, 80, 290],
            "股东户数-增减": [10, 0, 10],
            "股东户数-增减比例": [1.0, 0.0, 1.0],
            "股东户数公告日期": ["2025-08-30", "2025-04-26", None],
            "代码": ["600519", "600519", "600519"],
        }
    )
    normalized = normalize_holder_detail(frame, "600519")
    assert normalized["holder_count"].tolist() == [100]


def test_normalize_drops_mismatched_symbol_rows():
    frame = _raw_frame().assign(代码=["2594", "000001", "2594"])
    normalized = normalize_holder_detail(frame, "002594")
    assert len(normalized) == 2
    assert set(normalized["symbol"]) == {"002594"}


def test_normalize_empty_frame_returns_empty_canonical():
    normalized = normalize_holder_detail(pd.DataFrame(), "600519")
    assert list(normalized.columns) == list(OUTPUT_COLUMNS)
    assert normalized.empty


def test_normalize_missing_columns_raises():
    with pytest.raises(ValueError, match="缺少字段"):
        normalize_holder_detail(
            _raw_frame().drop(columns=["股东户数公告日期"]), "002594"
        )


def test_local_max_reads_tmp_partition(tmp_path, monkeypatch):
    collector, _ = _isolated_collector(
        tmp_path, monkeypatch, fetcher=lambda symbol: pd.DataFrame()
    )
    assert collector._get_local_max_stat_date("600519") == "1990-01-01"
    old = pd.DataFrame(
        {
            "stat_date": [
                pd.Timestamp("2024-12-31").date(),
                pd.Timestamp("2025-03-31").date(),
            ],
            "holder_count": [1, 2],
            "prev_holder_count": [0, 1],
            "holder_change": [1, 1],
            "holder_change_pct": [0.0, 0.0],
            "announce_date": [
                pd.Timestamp("2025-03-28").date(),
                pd.Timestamp("2025-04-26").date(),
            ],
            "symbol": ["600519", "600519"],
        }
    )
    collector.store.save_partition(old, "holder_number", "600519")
    assert collector._get_local_max_stat_date("600519") == "2025-03-31"


def _isolated_collector(tmp_path, monkeypatch, fetcher, cninfo_fetcher=None):
    import storage.file_store.parquet_store as store_mod

    monkeypatch.setattr(store_mod, "WAREHOUSE_DIR", str(tmp_path))
    monkeypatch.setattr(holder_mod, "_sleep_between_cninfo_fetches", lambda: None)
    statuses = []
    monkeypatch.setattr(
        holder_mod, "record_sync_success", lambda *args: statuses.append(args)
    )
    return (
        HolderCollector(
            fetcher=fetcher,
            cninfo_fetcher=cninfo_fetcher or (lambda quarter: pd.DataFrame()),
        ),
        statuses,
    )


def test_collect_merges_with_existing_partition(tmp_path, monkeypatch):
    collector, statuses = _isolated_collector(
        tmp_path, monkeypatch, fetcher=lambda symbol: _raw_frame()
    )
    # 预置本地旧分区 (含一条将被更新的重叠行 + 一条历史行)
    old = pd.DataFrame(
        {
            "stat_date": [
                pd.Timestamp("2024-12-31").date(),
                pd.Timestamp("2025-03-31").date(),
            ],
            "holder_count": [199192, 1],
            "prev_holder_count": [190000, 0],
            "holder_change": [9192, 1],
            "holder_change_pct": [4.8, 0.0],
            "announce_date": [
                pd.Timestamp("2025-03-28").date(),
                pd.Timestamp("2025-04-26").date(),
            ],
            "symbol": ["002594", "002594"],
        }
    )
    collector.store.save_partition(old, "holder_number", "002594")

    monkeypatch.setattr(
        collector, "_get_local_max_stat_date", lambda symbol: "2025-01-01"
    )
    collector.collect_holder_number("002594")

    saved = pd.read_parquet(
        tmp_path / "holder_number" / "symbol=002594" / "data.parquet"
    )
    assert saved["stat_date"].astype(str).tolist() == [
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
    ]
    # 重叠行被新值覆盖
    assert (
        saved.loc[saved["stat_date"].astype(str) == "2025-03-31", "holder_count"].iloc[
            0
        ]
        == 203700
    )
    assert statuses and statuses[0][0:2] == ("holder_number", "002594")


def test_collect_both_sources_empty_records_nothing(tmp_path, monkeypatch):
    """东财与巨潮皆空：不记成功 (下次重跑补抓)，不建分区。"""
    collector, statuses = _isolated_collector(
        tmp_path, monkeypatch, fetcher=lambda symbol: pd.DataFrame()
    )
    collector.collect_holder_number("600519")
    assert statuses == []
    assert not (tmp_path / "holder_number").exists()


def test_collect_start_date_filters_old_rows(tmp_path, monkeypatch):
    seen = []
    collector, _ = _isolated_collector(
        tmp_path, monkeypatch, fetcher=lambda symbol: _raw_frame()
    )
    orig_save = collector._save_incremental
    monkeypatch.setattr(
        collector,
        "_save_incremental",
        lambda df, symbol: (seen.append(df), orig_save(df, symbol)),
    )
    collector.collect_holder_number("002594", start_date="20250601")
    assert seen and seen[0]["stat_date"].astype(str).tolist() == ["2025-06-30"]


def test_main_sync_holder_counts_failure(monkeypatch):
    from data_ingestion.collectors import holder_collector

    class FakeHolderCollector:
        def collect_holder_number(self, *args, **kwargs):
            raise RuntimeError("户数接口异常")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(holder_collector, "HolderCollector", FakeHolderCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    assert main_mod.sync_holder_number(symbol="600519") == (1, 1)


def test_main_sync_holder_skips_synced_today(monkeypatch):
    from data_ingestion.collectors import holder_collector
    from storage.database import sync_status as sync_status_mod

    calls = []

    class FakeHolderCollector:
        def collect_holder_number(self, *args, **kwargs):
            calls.append(args)

    monkeypatch.setattr(main_mod, "get_active_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(holder_collector, "HolderCollector", FakeHolderCollector)
    monkeypatch.setattr(sync_status_mod, "is_synced_today", lambda *a, **k: True)
    assert main_mod.sync_holder_number() == (1, 0)
    assert calls == []


def test_holder_number_view_reads_only_atomic_data_files(monkeypatch):
    import storage.database.view_base as view_base_mod
    from storage.database.views.market.holder_number import HolderNumberView

    monkeypatch.setattr(
        view_base_mod, "ensure_schema", lambda dataset: {"stat_date": "DATE"}
    )
    sql = HolderNumberView().get_sql("/warehouse")
    assert HolderNumberView().name == "holder_number"
    assert "holder_number/*/data.parquet" in sql
    assert "*/*.parquet" not in sql


def _cninfo_frame(rows):
    """构造巨潮季度统计源格式的行。"""
    return pd.DataFrame(
        {
            "证券代码": [r[0] for r in rows],
            "证券简称": ["测试"] * len(rows),
            "变动日期": [r[1] for r in rows],
            "本期股东人数": [r[2] for r in rows],
            "上期股东人数": [r[3] for r in rows],
            "股东人数增幅": [r[4] for r in rows],
            "本期人均持股数量": [0] * len(rows),
            "上期人均持股数量": [0] * len(rows),
            "人均持股数量增幅": [0.0] * len(rows),
        }
    )


def test_normalize_cninfo_frame_maps_and_marks_source():
    normalized = normalize_cninfo_frame(
        _cninfo_frame([("601899", "2025-06-30", 335659, 369956.0, -9.27)]),
        "601899",
    )
    assert list(normalized.columns) == list(OUTPUT_COLUMNS)
    row = normalized.iloc[0]
    assert row["symbol"] == "601899"
    assert row["source"] == "cninfo"
    assert row["holder_count"] == 335659
    assert row["prev_holder_count"] == 369956.0
    assert row["holder_change"] == pytest.approx(335659 - 369956.0)
    assert row["holder_change_pct"] == -9.27
    assert str(row["stat_date"]) == "2025-06-30"
    assert str(row["announce_date"]) == "2025-08-31"


def test_normalize_cninfo_statutory_dates_cover_four_quarters():
    frame = _cninfo_frame(
        [
            ("600519", "2025-03-31", 1, 2.0, 0.0),
            ("600519", "2025-09-30", 1, 2.0, 0.0),
            ("600519", "2025-12-31", 1, 2.0, 0.0),
            ("600519", "2024-12-31", 1, 2.0, 0.0),
        ]
    )
    normalized = normalize_cninfo_frame(frame, "600519")
    mapping = dict(
        zip(
            normalized["stat_date"].astype(str),
            normalized["announce_date"].astype(str),
        )
    )
    assert mapping == {
        "2024-12-31": "2025-04-30",
        "2025-03-31": "2025-04-30",
        "2025-09-30": "2025-10-31",
        "2025-12-31": "2026-04-30",
    }


def test_normalize_cninfo_drops_mismatched_and_missing():
    frame = _cninfo_frame(
        [
            ("601899", "2025-06-30", 335659, 369956.0, -9.27),
            ("000001", "2025-06-30", 100, 90.0, 1.0),
            ("601899", "2025-03-31", None, 369956.0, 0.0),
        ]
    )
    normalized = normalize_cninfo_frame(frame, "601899")
    assert len(normalized) == 1
    assert normalized.iloc[0]["holder_count"] == 335659


def test_required_cninfo_quarters_boundaries():
    from datetime import date

    assert _required_cninfo_quarters(date(2026, 6, 30), date(2026, 9, 8)) == []
    assert _required_cninfo_quarters(date(2026, 3, 31), date(2026, 9, 8)) == [
        "20260630"
    ]
    quarters = _required_cninfo_quarters(date(1990, 1, 1), date(2026, 9, 8))
    assert quarters[0] == "20170331"
    assert quarters[-1] == "20260630"
    assert len(quarters) == 38


def _cninfo_by_quarter(mapping):
    def fetch(quarter):
        return mapping.get(quarter, pd.DataFrame())

    return fetch


def test_collect_falls_back_to_cninfo_when_em_missing(tmp_path, monkeypatch):
    def boom(symbol):
        raise TypeError("'NoneType' object is not subscriptable")

    cninfo = _cninfo_by_quarter(
        {
            "20250331": _cninfo_frame(
                [("601899", "2025-03-31", 369956, 350000.0, 5.7)]
            ),
            "20250630": _cninfo_frame(
                [("601899", "2025-06-30", 335659, 369956.0, -9.27)]
            ),
        }
    )
    collector, statuses = _isolated_collector(
        tmp_path, monkeypatch, fetcher=boom, cninfo_fetcher=cninfo
    )
    monkeypatch.setattr(
        holder_mod, "_required_cninfo_quarters", lambda after: ["20250331", "20250630"]
    )
    collector.collect_holder_number("601899")

    saved = pd.read_parquet(
        tmp_path / "holder_number" / "symbol=601899" / "data.parquet"
    )
    assert saved["stat_date"].astype(str).tolist() == ["2025-03-31", "2025-06-30"]
    assert set(saved["source"]) == {"cninfo"}
    assert statuses and statuses[0][0:2] == ("holder_number", "601899")


def test_collect_skips_cninfo_fetch_when_history_complete(tmp_path, monkeypatch):
    """东财史完备时巨潮零请求（稳态零成本）。"""
    from datetime import date

    quarters = holder_mod._required_cninfo_quarters(date(1990, 1, 1))
    rows = {
        "股东户数统计截止日": [],
        "股东户数-本次": [],
        "股东户数-上次": [],
        "股东户数-增减": [],
        "股东户数-增减比例": [],
        "股东户数公告日期": [],
        "代码": [],
    }
    for q in quarters:
        d = f"{q[:4]}-{q[4:6]}-{q[6:]}"
        rows["股东户数统计截止日"].append(d)
        rows["股东户数-本次"].append(100)
        rows["股东户数-上次"].append(90)
        rows["股东户数-增减"].append(10)
        rows["股东户数-增减比例"].append(1.0)
        rows["股东户数公告日期"].append(d)
        rows["代码"].append("600519")
    calls = []
    collector, statuses = _isolated_collector(
        tmp_path,
        monkeypatch,
        fetcher=lambda symbol: pd.DataFrame(rows),
        cninfo_fetcher=lambda quarter: calls.append(quarter) or pd.DataFrame(),
    )
    collector.collect_holder_number("600519")
    assert calls == []
    assert statuses and statuses[0][0:2] == ("holder_number", "600519")


def test_collect_fills_middle_quarter_hole_from_cninfo(tmp_path, monkeypatch):
    """东财中部缺季度时巨潮补洞，东财行优先。"""
    em_row = {
        "股东户数统计截止日": ["2025-06-30"],
        "股东户数-本次": [323841],
        "股东户数-上次": [203606],
        "股东户数-增减": [120235],
        "股东户数-增减比例": [59.05],
        "股东户数公告日期": ["2025-08-30"],
        "代码": ["002594"],
    }
    cninfo = _cninfo_by_quarter(
        {
            "20250331": _cninfo_frame(
                [("002594", "2025-03-31", 203606, 199192.0, 2.21)]
            ),
        }
    )
    collector, _ = _isolated_collector(
        tmp_path,
        monkeypatch,
        fetcher=lambda symbol: pd.DataFrame(em_row),
        cninfo_fetcher=cninfo,
    )
    legacy = pd.DataFrame(
        {
            "stat_date": [pd.Timestamp("2024-12-31").date()],
            "holder_count": [199192],
            "prev_holder_count": [190000],
            "holder_change": [9192],
            "holder_change_pct": [4.8],
            "announce_date": [pd.Timestamp("2025-03-28").date()],
            "symbol": ["002594"],
        }
    )
    collector.store.save_partition(legacy, "holder_number", "002594")
    collector.collect_holder_number("002594")

    saved = pd.read_parquet(
        tmp_path / "holder_number" / "symbol=002594" / "data.parquet"
    )
    assert saved["stat_date"].astype(str).tolist() == [
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
    ]
    by_date = dict(zip(saved["stat_date"].astype(str), saved["source"]))
    assert by_date == {
        "2024-12-31": "em",
        "2025-03-31": "cninfo",
        "2025-06-30": "em",
    }


def test_collect_raises_when_both_sources_fail(tmp_path, monkeypatch):
    def boom(*args):
        raise RuntimeError("接口异常")

    collector, statuses = _isolated_collector(
        tmp_path, monkeypatch, fetcher=boom, cninfo_fetcher=boom
    )
    with pytest.raises(RuntimeError, match="接口异常"):
        collector.collect_holder_number("601899")
    assert statuses == []


def test_save_keeps_unknown_counts_null(tmp_path, monkeypatch):
    """未知户数保持 NULL，禁止写成 0（下游 0=不变 ≠ 未知）。"""
    collector, _ = _isolated_collector(
        tmp_path,
        monkeypatch,
        fetcher=lambda symbol: (_ for _ in ()).throw(AssertionError("不应调东财")),
        cninfo_fetcher=_cninfo_by_quarter(
            {
                "20250630": _cninfo_frame(
                    [("601899", "2025-06-30", 335659, None, -9.27)]
                ),
            }
        ),
    )
    monkeypatch.setattr(
        holder_mod, "_required_cninfo_quarters", lambda after: ["20250630"]
    )
    collector.collect_holder_number("601899")

    saved = pd.read_parquet(
        tmp_path / "holder_number" / "symbol=601899" / "data.parquet"
    )
    assert len(saved) == 1
    assert pd.isna(saved.iloc[0]["prev_holder_count"])
    assert pd.isna(saved.iloc[0]["holder_change"])
    assert saved.iloc[0]["holder_count"] == 335659


def test_collect_invalid_start_date_records_nothing(tmp_path, monkeypatch):
    collector, statuses = _isolated_collector(
        tmp_path, monkeypatch, fetcher=lambda symbol: _raw_frame()
    )
    collector.collect_holder_number("002594", start_date="not-a-date")
    assert statuses == []
    assert not (tmp_path / "holder_number").exists()


def test_statutory_announce_date_non_standard_quarter():
    from datetime import date

    assert holder_mod._statutory_announce_date(date(2025, 2, 28)) == date(2025, 3, 31)


def test_last_completed_quarter_end_year_boundary():
    from datetime import date

    assert holder_mod._last_completed_quarter_end(date(2026, 1, 1)) == date(
        2025, 12, 31
    )
    assert holder_mod._required_cninfo_quarters(
        date(2025, 9, 30), date(2026, 1, 15)
    ) == ["20251231"]


def test_main_sync_holder_empty_data_counts_processed_not_failed(monkeypatch):
    """两源皆空：返回 (1,0) 不记成功（空≠成功，下次重跑补抓）。"""
    from data_ingestion.collectors import holder_collector

    class FakeHolderCollector:
        def collect_holder_number(self, *args, **kwargs):
            return None

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(holder_collector, "HolderCollector", FakeHolderCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    assert main_mod.sync_holder_number(symbol="600519") == (1, 0)


def test_collect_prefers_em_over_cninfo_for_same_quarter(tmp_path, monkeypatch):
    """同季度东财优先：东财全量已有某季度时，巨潮不再重复拉取合并。"""
    cninfo = _cninfo_by_quarter(
        {
            "20250331": _cninfo_frame([("002594", "2025-03-31", 1, 2.0, 0.0)]),
            "20250630": _cninfo_frame([("002594", "2025-06-30", 2, 1.0, 0.0)]),
        }
    )
    collector, statuses = _isolated_collector(
        tmp_path,
        monkeypatch,
        fetcher=lambda symbol: _raw_frame(),
        cninfo_fetcher=cninfo,
    )
    # _raw_frame 含 2025-03-31/2025-06-30；显式起点只收 06-30，
    # 但补缺判定仍以东财全量为准，03-31 不应被巨潮行覆盖
    collector.collect_holder_number("002594", start_date="20250601")

    saved = pd.read_parquet(
        tmp_path / "holder_number" / "symbol=002594" / "data.parquet"
    )
    assert saved["stat_date"].astype(str).tolist() == ["2025-06-30"]
    assert set(saved["source"]) == {"em"}
    assert statuses and statuses[0][0:2] == ("holder_number", "002594")


def test_save_backfills_source_em_for_legacy_rows(tmp_path, monkeypatch):
    collector, statuses = _isolated_collector(
        tmp_path, monkeypatch, fetcher=lambda symbol: _raw_frame().iloc[[0]]
    )
    legacy = pd.DataFrame(
        {
            "stat_date": [pd.Timestamp("2025-03-31").date()],
            "holder_count": [203606],
            "prev_holder_count": [199192],
            "holder_change": [4414],
            "holder_change_pct": [2.21],
            "announce_date": [pd.Timestamp("2025-04-26").date()],
            "symbol": ["002594"],
        }
    )
    collector.store.save_partition(legacy, "holder_number", "002594")
    collector.collect_holder_number("002594")

    saved = pd.read_parquet(
        tmp_path / "holder_number" / "symbol=002594" / "data.parquet"
    )
    assert set(saved["source"]) == {"em"}
