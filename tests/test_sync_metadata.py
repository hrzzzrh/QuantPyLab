"""单元测试: sync-metadata 行业雪球补全 (mock 雪球接口, 不触真实网络)"""

import threading
from unittest import mock

import pytest

import main as main_mod
from storage.database import manager as manager_mod


@pytest.fixture(autouse=True)
def _isolate_db(tmp_path, monkeypatch):
    sqlite_path = tmp_path / "test_metadata.db"
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    monkeypatch.setattr(manager_mod.db_manager, "sqlite_path", sqlite_path)
    manager_mod.db_manager._sqlite_conn = None
    manager_mod.db_manager.initialize_schema()
    conn = manager_mod.db_manager.get_sqlite_conn()
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, industry, is_active) VALUES"
        " ('600519', '600519', '贵州茅台', NULL, 1)"
    )
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, industry, is_active) VALUES"
        " ('000001', '000001', '平安银行', '银行', 1)"
    )
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, industry, is_active) VALUES"
        " ('600421', '600421', '*ST华嵘', NULL, 0)"
    )
    conn.commit()
    yield
    manager_mod.db_manager._sqlite_conn = None


def _fake_collector(monkeypatch, results):
    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            if symbol in results:
                return {"industry_xq": results[symbol]}
            return {}

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod, "time", mock.Mock())
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)


def test_sync_industries_fills_missing_active_only(monkeypatch):
    """仅补全 industry 为 NULL 的活跃股, 跳过已有行业与退市股"""
    _fake_collector(monkeypatch, {"sh600519": "白酒"})
    conn = manager_mod.db_manager.get_sqlite_conn()
    main_mod._sync_industries_via_xueqiu(conn)

    rows = dict(conn.execute("SELECT symbol, industry FROM stocks").fetchall())
    assert rows["600519"] == "白酒"
    assert rows["000001"] == "银行"
    assert rows["600421"] is None


def test_sync_industries_skips_failed(monkeypatch):
    """雪球无资料 (退市股/北交所) 不计异常失败, 不写入"""
    _fake_collector(monkeypatch, {})
    conn = manager_mod.db_manager.get_sqlite_conn()
    assert main_mod._sync_industries_via_xueqiu(conn) == (1, 0)
    rows = dict(conn.execute("SELECT symbol, industry FROM stocks").fetchall())
    assert rows["600519"] is None


def test_sync_industries_counts_worker_exception(monkeypatch):
    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            raise RuntimeError("雪球接口异常")

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    conn = manager_mod.db_manager.get_sqlite_conn()
    assert main_mod._sync_industries_via_xueqiu(conn) == (1, 1)


def test_sync_list_info_falls_back_to_cninfo_on_main_thread(monkeypatch):
    """巨潮兜底只在主线程串行执行

    akshare 巨潮接口依赖 V8 引擎 (py_mini_racer.MiniRacer), 其构造/使用
    非线程安全, 并发调用会触发 FATAL 崩溃 (Check failed: !pool->IsInitialized())。
    雪球/东财返回空时, 兜底必须发生在主线程而非 worker 线程。
    """
    cninfo_call_threads = []

    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            return {}

        def fetch_from_eastmoney(self, code):
            return {}

        def fetch_from_cninfo(self, code):
            cninfo_call_threads.append(threading.current_thread().name)
            return {"area": "贵州", "list_date": "20200101"}

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)

    conn = manager_mod.db_manager.get_sqlite_conn()
    main_mod.sync_stock_metadata(run_industry=False, run_list_info=True)

    assert cninfo_call_threads, "雪球/东财均空时巨潮兜底应被调用"
    assert set(cninfo_call_threads) == {"MainThread"}, (
        f"巨潮兜底必须在主线程执行, 实际调用线程: {set(cninfo_call_threads)}"
    )
    rows = conn.execute("SELECT symbol, area, list_date FROM stocks").fetchall()
    row_map = {r[0]: (r[1], r[2]) for r in rows}
    assert row_map["600519"] == (None, "20200101")


def test_list_info_falls_back_to_sina_klc_for_list_date(monkeypatch):
    """雪球/东财/巨潮均无数据时, 用 Sina klc_kl.js 首条日期兜底 list_date"""
    from datetime import date

    import utils.sina_klc as sina_klc_mod
    from storage.database.sync_status import DATASET_STOCK_METADATA, get_last_sync_date

    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            return {}

        def fetch_from_eastmoney(self, code):
            return {}

        def fetch_from_cninfo(self, code):
            return {"area": "贵州"}

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        sina_klc_mod.SinaKlcFetcher,
        "fetch_list_date",
        staticmethod(lambda code: "19930607"),
    )

    conn = manager_mod.db_manager.get_sqlite_conn()
    main_mod.sync_stock_metadata(run_industry=False, run_list_info=True)

    rows = conn.execute("SELECT symbol, area, list_date FROM stocks").fetchall()
    row_map = {r[0]: (r[1], r[2]) for r in rows}
    assert row_map["600519"] == (None, "19930607")
    assert get_last_sync_date(DATASET_STOCK_METADATA, "600519") == date.today()


def test_list_info_does_not_record_incomplete_metadata(monkeypatch):
    """仍缺少必需字段时不得记录成功, 下次同步应继续补全"""
    from storage.database.sync_status import DATASET_STOCK_METADATA, get_last_sync_date

    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            return {"area": "贵州"}

        def fetch_from_eastmoney(self, code):
            return {}

        def fetch_from_cninfo(self, code):
            return {}

    import utils.sina_klc as sina_klc_mod

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        sina_klc_mod.SinaKlcFetcher, "fetch_list_date", lambda code: None
    )

    main_mod.sync_stock_metadata(run_industry=False, run_list_info=True)

    assert get_last_sync_date(DATASET_STOCK_METADATA, "600519") is None


def test_list_info_isolates_sina_klc_failure_per_stock(monkeypatch):
    import utils.sina_klc as sina_klc_mod

    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            return {}

        def fetch_from_eastmoney(self, code):
            return {}

        def fetch_from_cninfo(self, code):
            return {"area": "贵州"}

    def fail_for_one_stock(code):
        if code == "600519":
            raise sina_klc_mod.SinaKlcFetchError("invalid date")
        return "19930607"

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        sina_klc_mod.SinaKlcFetcher, "fetch_list_date", fail_for_one_stock
    )

    conn = manager_mod.db_manager.get_sqlite_conn()
    processed, failed = main_mod.sync_stock_metadata(
        run_industry=False, run_list_info=True
    )

    assert processed == 3
    assert failed == 1
    row = conn.execute(
        "SELECT area, list_date FROM stocks WHERE symbol = '600421'"
    ).fetchone()
    assert row == (None, "19930607")


def test_list_info_retries_missing_metadata_even_if_synced(monkeypatch):
    """元数据不完整时即使有历史成功记录也必须继续补全"""
    from datetime import date

    from storage.database.sync_status import DATASET_STOCK_METADATA, record_sync_success

    conn = manager_mod.db_manager.get_sqlite_conn()
    conn.execute(
        "UPDATE stocks SET area = '上海', list_date = '19910403'"
        " WHERE symbol = '000001'"
    )
    conn.execute(
        "UPDATE stocks SET area = '上海', list_date = NULL WHERE symbol = '600519'"
    )
    conn.commit()
    for sym in ("600519", "000001", "600421"):
        record_sync_success(DATASET_STOCK_METADATA, sym, date.today())

    called = []

    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            called.append(symbol)
            return {"area": "贵州", "list_date": "20200101"}

        def fetch_from_eastmoney(self, code):
            return {}

        def fetch_from_cninfo(self, code):
            return {}

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)

    main_mod.sync_stock_metadata(run_industry=False, run_list_info=True)

    assert set(called) == {"sh600519", "sh600421"}
    rows = conn.execute("SELECT symbol, area, list_date FROM stocks").fetchall()
    row_map = {r[0]: (r[1], r[2]) for r in rows}
    assert row_map["600519"] == ("上海", "20200101")
    assert row_map["000001"] == ("上海", "19910403")


def test_list_info_normalizes_blank_and_nan_values(monkeypatch):
    """空字符串和 NaN 必须按缺失值保存, 且不得记录同步成功"""
    from storage.database.sync_status import DATASET_STOCK_METADATA, get_last_sync_date

    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            return {
                "area": " ",
                "list_date": float("nan"),
                "industry_xq": float("nan"),
            }

        def fetch_from_eastmoney(self, code):
            return {}

        def fetch_from_cninfo(self, code):
            return {}

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)

    conn = manager_mod.db_manager.get_sqlite_conn()
    main_mod.sync_stock_metadata(run_industry=False, run_list_info=True)

    row = conn.execute(
        "SELECT area, list_date, industry FROM stocks WHERE symbol = '600519'"
    ).fetchone()
    assert row == (None, None, None)
    assert get_last_sync_date(DATASET_STOCK_METADATA, "600519") is None
