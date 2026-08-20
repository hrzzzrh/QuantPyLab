"""单元测试: 退市股 K线 Sina 全量重建 (mock 网络与数据库, 不触真实网络)"""

from datetime import date

import pandas as pd
import pytest

from data_ingestion.collectors.kline_collector import (
    DailyKlineCollector,
    KlineDataUnavailableError,
)
from storage.database import manager as manager_mod
from storage.database.sync_status import (
    DATASET_KLINE,
    DATASET_KLINE_DAILY_NO_DATA,
    get_last_sync_date,
    record_sync_success,
)


@pytest.fixture(autouse=True)
def _isolate_env(tmp_path, monkeypatch):
    """隔离 SQLite 元数据库与 Parquet 仓库目录"""
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
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES ('600421', '600421', '*ST华嵘', 0)"
    )
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES ('600519', '600519', '贵州茅台', 1)"
    )
    conn.commit()
    yield
    ss.db_manager._sqlite_conn = None


def test_is_delisted():
    collector = DailyKlineCollector()
    assert collector._is_delisted("600421") is True
    assert collector._is_delisted("600519") is False
    assert collector._is_delisted("000001") is False


def test_delisted_route_falls_back_to_tencent_after_empty_sina(monkeypatch):
    collector = DailyKlineCollector()
    called = {"sina": False}

    def fake_delisted(symbol):
        return symbol == "600421"

    def fake_sina(sina_symbol, start_date="19900101", end_date=None):
        called["sina"] = True
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_is_delisted", fake_delisted)
    monkeypatch.setattr(collector, "_fetch_sina_klc", fake_sina)
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: (_ for _ in ()).throw(KlineDataUnavailableError("腾讯测试失败")),
    )
    with pytest.raises(KlineDataUnavailableError, match="腾讯测试失败"):
        collector.collect_kline("600421")
    assert called["sina"] is True


def test_delisted_rebuild_and_mark(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: pd.DataFrame(
            {
                "date": ["2026-04-28", "2026-04-29"],
                "open": [4.29, 4.08],
                "high": [4.29, 4.08],
                "low": [4.29, 4.08],
                "close": [4.29, 4.08],
                "volume": [100.0, 200.0],
                "amount": [42900.0, 81600.0],
                "close_hfq": [4.29, 4.08],
                "adj_factor": [1.0, 1.0],
            }
        ),
    )
    saved = {}
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda df, cat, sym: saved.update({"cat": cat, "sym": sym, "n": len(df)}),
    )

    result = collector.collect_kline("600421")
    assert result is True
    assert saved == {"cat": "daily_kline", "sym": "600421", "n": 2}
    assert get_last_sync_date(DATASET_KLINE, "600421") == date.today()

    conn = manager_mod.db_manager.get_sqlite_conn()
    row = conn.execute(
        "SELECT last_trade_date FROM stocks WHERE symbol='600421'"
    ).fetchone()
    assert row[0] == "20260429"


def test_delisted_rebuild_preserves_existing_pre_cutoff_rows(monkeypatch):
    collector = DailyKlineCollector()
    path = collector.store.base_dir / "daily_kline" / "symbol=600421" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "date": ["2009-12-31"],
            "open": [3.0],
            "high": [3.2],
            "low": [2.8],
            "close": [3.1],
            "volume": [100.0],
            "amount": [310.0],
            "adj_factor": [1.0],
        }
    ).to_parquet(path, index=False)
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "date": ["2026-04-29"],
                "open": [4.08],
                "high": [4.1],
                "low": [4.0],
                "close": [4.05],
                "volume": [200.0],
                "amount": [81600.0],
                "close_hfq": [4.05],
                "adj_factor": [1.0],
            }
        ),
    )

    assert collector.collect_kline("600421") is True

    saved = pd.read_parquet(path)
    assert saved["date"].astype(str).tolist() == ["2009-12-31", "2026-04-29"]


def test_delisted_rebuild_filters_pre_cutoff_source_rows(monkeypatch):
    collector = DailyKlineCollector()
    path = collector.store.base_dir / "daily_kline" / "symbol=600421" / "data.parquet"
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "date": ["2009-12-31", "2026-04-29"],
                "open": [3.08, 4.08],
                "high": [3.1, 4.1],
                "low": [3.0, 4.0],
                "close": [3.05, 4.05],
                "volume": [100.0, 200.0],
                "amount": [305.0, 81600.0],
                "close_hfq": [3.05, 4.05],
                "adj_factor": [1.0, 1.0],
            }
        ),
    )

    assert collector.collect_kline("600421") is True

    saved = pd.read_parquet(path)
    assert saved["date"].astype(str).tolist() == ["2026-04-29"]


def test_delisted_partition_excludes_derived_close_hfq(monkeypatch):
    collector = DailyKlineCollector()
    saved = {}
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "date": ["2026-04-29"],
                "open": [4.08],
                "high": [4.08],
                "low": [4.08],
                "close": [4.08],
                "volume": [200.0],
                "amount": [81600.0],
                "close_hfq": [4.08],
                "adj_factor": [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda frame, category, symbol: saved.update(
            {"columns": list(frame.columns), "category": category, "symbol": symbol}
        ),
    )

    assert collector.collect_kline("600421") is True
    assert saved["columns"] == [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "adj_factor",
    ]


def test_delisted_rebuild_uses_tencent_after_invalid_sina(monkeypatch):
    collector = DailyKlineCollector()
    invalid = pd.DataFrame(
        {
            "date": ["2026-04-29"],
            "open": [4.0],
            "high": [4.1],
            "low": [4.2],
            "close": [4.05],
            "volume": [200.0],
            "amount": [81600.0],
            "close_hfq": [4.05],
            "adj_factor": [1.0],
        }
    )
    tencent = pd.DataFrame(
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
    tencent.attrs["source"] = "tencent-newfq"
    calls = []
    monkeypatch.setattr(collector, "_fetch_sina_klc", lambda *args: invalid)
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: calls.append(symbol) or tencent,
    )
    monkeypatch.setattr(collector.store, "save_partition", lambda *args: None)

    assert collector.collect_kline("600421") is True
    assert calls == ["600421"]


def test_delisted_status_retry_does_not_refetch(monkeypatch):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    fetch_calls = []
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda *args, **kwargs: (
            fetch_calls.append(1)
            or pd.DataFrame(
                {
                    "date": ["2026-04-29"],
                    "open": [4.08],
                    "high": [4.08],
                    "low": [4.08],
                    "close": [4.08],
                    "volume": [200.0],
                    "amount": [81600.0],
                    "close_hfq": [4.08],
                    "adj_factor": [1.0],
                }
            )
        ),
    )
    monkeypatch.setattr(collector.store, "save_partition", lambda *args: None)
    original_record = kline_mod.record_sync_success
    attempts = []

    def fail_once(dataset, symbol, sync_date):
        if dataset == DATASET_KLINE:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("metadata unavailable")
        return original_record(dataset, symbol, sync_date)

    monkeypatch.setattr(kline_mod, "record_sync_success", fail_once)

    result = collector.collect_kline("600421")

    assert result is True
    assert fetch_calls == [1]
    assert attempts == [1, 1]
    assert get_last_sync_date(DATASET_KLINE, "600421") == date.today()


def test_delisted_pending_status_skips_delisted_query(monkeypatch):
    collector = DailyKlineCollector()
    collector._pending_rebuild_status_symbols.add("600421")
    monkeypatch.setattr(
        collector,
        "_is_delisted",
        lambda symbol: (_ for _ in ()).throw(AssertionError("不应查询退市状态")),
    )

    result = collector.collect_kline("600421")

    assert result is True
    assert get_last_sync_date(DATASET_KLINE, "600421") == date.today()


def test_delisted_invalid_klc_frame_falls_back_to_tencent(monkeypatch):
    from utils import retry as retry_mod

    collector = DailyKlineCollector()
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "date": ["2026-04-29"],
                "open": [4.0],
                "high": [4.1],
                "low": [4.2],
                "close": [4.05],
                "volume": [200.0],
                "amount": [81600.0],
                "close_hfq": [4.05],
                "adj_factor": [1.0],
            }
        ),
    )
    saved = []
    monkeypatch.setattr(
        collector.store, "save_partition", lambda *args: saved.append(args)
    )
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: (_ for _ in ()).throw(KlineDataUnavailableError("腾讯测试失败")),
    )

    with pytest.raises(KlineDataUnavailableError, match="腾讯测试失败"):
        collector.collect_kline("600421")

    assert saved == []
    assert get_last_sync_date(DATASET_KLINE, "600421") is None


def test_delisted_skips_when_rebuilt(monkeypatch):
    collector = DailyKlineCollector()
    record_sync_success(DATASET_KLINE, "600421", date.today())
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: (_ for _ in ()).throw(
            AssertionError("不应抓取")
        ),
    )
    result = collector.collect_kline("600421")
    assert result is False


def test_delisted_sina_empty_returns_false(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: pd.DataFrame(),
    )
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_newfq",
        lambda symbol: (_ for _ in ()).throw(KlineDataUnavailableError("腾讯测试失败")),
    )
    with pytest.raises(KlineDataUnavailableError, match="腾讯测试失败"):
        collector.collect_kline("600421")
    assert get_last_sync_date(DATASET_KLINE, "600421") is None


def test_delisted_no_post_cutoff_data_records_no_data(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(
        collector, "_fetch_sina_klc", lambda *args, **kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        collector, "_fetch_tencent_newfq", lambda symbol: pd.DataFrame()
    )

    assert collector.collect_kline("600421") is False
    assert get_last_sync_date(DATASET_KLINE, "600421") is None
    assert get_last_sync_date(DATASET_KLINE_DAILY_NO_DATA, "600421") == date.today()


def test_pending_delisted_no_data_status_does_not_refetch(monkeypatch):
    collector = DailyKlineCollector()
    collector._pending_daily_no_data_status_symbols.add("600421")
    monkeypatch.setattr(
        collector,
        "_is_delisted",
        lambda symbol: (_ for _ in ()).throw(AssertionError("不应查询退市状态")),
    )

    assert collector.collect_kline("600421") is False
    assert get_last_sync_date(DATASET_KLINE_DAILY_NO_DATA, "600421") == date.today()


def test_rebuild_success_clears_no_data_status():
    collector = DailyKlineCollector()
    record_sync_success(DATASET_KLINE_DAILY_NO_DATA, "600421", date.today())

    collector._record_rebuild_success("600421")

    assert get_last_sync_date(DATASET_KLINE_DAILY_NO_DATA, "600421") is None


@pytest.mark.parametrize(
    ("method_name", "dataset", "pending_attribute"),
    [
        (
            "_record_daily_success",
            "kline_daily",
            "_pending_daily_status_symbols",
        ),
        (
            "_record_rebuild_success",
            DATASET_KLINE,
            "_pending_rebuild_status_symbols",
        ),
    ],
)
def test_success_status_clear_failure_is_pending_and_retried(
    monkeypatch, method_name, dataset, pending_attribute
):
    import data_ingestion.collectors.kline_collector as kline_mod

    collector = DailyKlineCollector()
    record_sync_success(DATASET_KLINE_DAILY_NO_DATA, "600421", date.today())
    original_clear = kline_mod.clear_sync_status
    attempts = []

    def fail_once(clear_dataset, symbol):
        attempts.append((clear_dataset, symbol))
        if len(attempts) == 1:
            raise RuntimeError("metadata unavailable")
        return original_clear(clear_dataset, symbol)

    monkeypatch.setattr(kline_mod, "clear_sync_status", fail_once)

    with pytest.raises(RuntimeError, match="metadata unavailable"):
        getattr(collector, method_name)("600421")

    assert "600421" in getattr(collector, pending_attribute)
    getattr(collector, method_name)("600421")

    assert "600421" not in getattr(collector, pending_attribute)
    assert attempts == [
        (DATASET_KLINE_DAILY_NO_DATA, "600421"),
        (DATASET_KLINE_DAILY_NO_DATA, "600421"),
    ]
    assert get_last_sync_date(dataset, "600421") == date.today()
    assert get_last_sync_date(DATASET_KLINE_DAILY_NO_DATA, "600421") is None


def test_last_trade_date_always_updated(monkeypatch):
    collector = DailyKlineCollector()
    conn = manager_mod.db_manager.get_sqlite_conn()
    conn.execute("UPDATE stocks SET last_trade_date='20260401' WHERE symbol='600421'")
    conn.commit()

    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: pd.DataFrame(
            {
                "date": ["2026-06-22"],
                "open": [0.24],
                "high": [0.24],
                "low": [0.24],
                "close": [0.24],
                "volume": [200.0],
                "amount": [1.0],
                "close_hfq": [0.24],
                "adj_factor": [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda df, cat, sym: None,
    )
    collector.collect_kline("600421")
    row = conn.execute(
        "SELECT last_trade_date FROM stocks WHERE symbol='600421'"
    ).fetchone()
    assert row[0] == "20260622"
