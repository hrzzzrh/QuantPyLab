"""单元测试: tools/schedule_sync_all.py 定时调度入口 (mock 全部外部依赖)"""

import types
from datetime import date

import pytest

import tools.schedule_sync_all as sched
from main import SYNC_ALL_BLOCKED, SYNC_ALL_RETRYABLE, SYNC_ALL_SUCCESS
from storage.database.sync_status import DATASET_SYNC_ALL, SYMBOL_SYNC_ALL


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """默认环境: 固定重试参数、禁用真实 sleep、卷已挂载、前一天是交易日、未记录成功"""
    monkeypatch.setattr(sched, "SYNC_ALL_MAX_RETRIES", 2)
    monkeypatch.setattr(sched, "SYNC_STATUS_MAX_RETRIES", 2)
    monkeypatch.setattr(sched, "SYNC_ALL_RETRY_INTERVAL_SECONDS", 0)
    # 仅替换 sched 命名空间内的 time 引用, 不影响全局 time 模块
    monkeypatch.setattr(sched, "time", types.SimpleNamespace(sleep=lambda _: None))
    monkeypatch.setattr(sched, "is_trade_date", lambda d: True)
    monkeypatch.setattr(sched, "get_last_sync_date", lambda d, s: None)
    monkeypatch.setattr(sched, "record_sync_success", lambda *a: None)
    monkeypatch.setattr(sched, "install_requests_protection", lambda: None)


def test_volume_missing_exits_without_sync(monkeypatch):
    monkeypatch.setattr(sched, "PROJECT_ROOT", sched.Path("/nonexistent-unittest-dir"))
    called = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: called.append(1) or SYNC_ALL_SUCCESS,
    )
    monkeypatch.setattr(sched, "record_sync_success", lambda *a: called.append(2))
    monkeypatch.setattr(sched, "install_requests_protection", lambda: called.append(3))
    assert sched.run_sync_all_with_retry() == 1
    assert called == []


def test_sync_status_read_and_write_retry_after_transient_failure(monkeypatch):
    read_calls = []
    write_calls = []

    def fake_get_last_sync_date(*args):
        read_calls.append(args)
        if len(read_calls) == 1:
            raise RuntimeError("database is locked")
        return None

    def fake_record_sync_success(*args):
        write_calls.append(args)
        if len(write_calls) == 1:
            raise RuntimeError("database is locked")

    monkeypatch.setattr(sched, "get_last_sync_date", fake_get_last_sync_date)
    monkeypatch.setattr(sched, "record_sync_success", fake_record_sync_success)
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: SYNC_ALL_SUCCESS,
    )

    assert sched.run_sync_all_with_retry() == 0
    assert len(read_calls) == 2
    assert len(write_calls) == 2


def test_pipeline_and_sync_status_use_independent_retry_limits(monkeypatch):
    """生产配置下流水线只跑一次，但瞬时 SQLite 锁仍可独立重试。"""
    monkeypatch.setattr(sched, "SYNC_ALL_MAX_RETRIES", 0)
    monkeypatch.setattr(sched, "SYNC_STATUS_MAX_RETRIES", 1)
    read_calls = []

    def fake_get_last_sync_date(*args):
        read_calls.append(args)
        if len(read_calls) == 1:
            raise RuntimeError("database is locked")
        return None

    flow_calls = []
    monkeypatch.setattr(sched, "get_last_sync_date", fake_get_last_sync_date)
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_RETRYABLE,
    )

    assert sched.run_sync_all_with_retry() == 1
    assert len(read_calls) == 2
    assert flow_calls == [1]


def test_prev_day_not_trade_day_exits(monkeypatch):
    monkeypatch.setattr(sched, "is_trade_date", lambda d: False)
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    installed = []
    monkeypatch.setattr(
        sched, "install_requests_protection", lambda: installed.append(1)
    )
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == []
    assert installed == [1]


def test_trade_calendar_unavailable_is_failure(monkeypatch):
    from utils.trade_date import TradeCalendarUnavailableError

    monkeypatch.setattr(
        sched,
        "is_trade_date",
        lambda d: (_ for _ in ()).throw(TradeCalendarUnavailableError("无缓存")),
    )
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    assert sched.run_sync_all_with_retry() == 1
    assert flow_calls == []


def test_already_synced_prev_day_exits(monkeypatch):
    monkeypatch.setattr(sched, "get_last_sync_date", lambda d, s: date.today())
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    monkeypatch.setattr(sched, "record_sync_success", lambda *a: flow_calls.append(2))
    installed = []
    monkeypatch.setattr(
        sched, "install_requests_protection", lambda: installed.append(1)
    )
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == []
    assert installed == [1]


def test_prev_day_synced_exits(monkeypatch):
    """记录日期等于前一天 (含跨周末场景) 时跳过"""
    yesterday = date.today() - sched.timedelta(days=1)
    monkeypatch.setattr(sched, "get_last_sync_date", lambda d, s: yesterday)
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    monkeypatch.setattr(sched, "record_sync_success", lambda *a: flow_calls.append(2))
    installed = []
    monkeypatch.setattr(
        sched, "install_requests_protection", lambda: installed.append(1)
    )
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == []
    assert installed == [1]


def test_stale_sync_record_triggers_run(monkeypatch):
    """记录日期早于前一天 (前一天未同步成功) 时执行流水线"""
    monkeypatch.setattr(
        sched,
        "get_last_sync_date",
        lambda d, s: date.today() - sched.timedelta(days=3),
    )
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    installed = []
    monkeypatch.setattr(
        sched, "install_requests_protection", lambda: installed.append(1)
    )
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1]
    assert installed == [1]


def test_success_records_prev_day(monkeypatch):
    """流水线成功时应记录数据日 (前一天), 保证每日数据零延迟"""
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda *args, **kwargs: SYNC_ALL_SUCCESS,
    )
    recorded = []
    monkeypatch.setattr(
        sched, "record_sync_success", lambda d, s, dt: recorded.append((d, s, dt))
    )
    installed = []
    monkeypatch.setattr(
        sched, "install_requests_protection", lambda: installed.append(1)
    )
    assert sched.run_sync_all_with_retry() == 0
    expected = (
        DATASET_SYNC_ALL,
        SYMBOL_SYNC_ALL,
        date.today() - sched.timedelta(days=1),
    )
    assert recorded == [expected]
    assert installed == [1]


def test_consecutive_days_no_data_lag(monkeypatch):
    """跨两日连续触发: 记录数据日使次日正常执行, 数据零延迟 (回归防护)

    旧实现记录运行日: D1 记录 08-11 后, D2 判定 prev=08-11 >= last=08-11 会
    错误跳过, 导致 08-11 的数据延迟一个交易日入库。本测试显式覆盖该场景:
    D2 必须执行流水线且记录新的数据日。
    """

    class _FakeDate(date):
        current = date(2026, 8, 11)

        @classmethod
        def today(cls):
            return cls.current

    monkeypatch.setattr(sched, "date", _FakeDate)

    last_recorded = {}

    def fake_get(d, s):
        return last_recorded.get("date")

    def fake_record(d, s, dt):
        last_recorded["date"] = dt

    monkeypatch.setattr(sched, "get_last_sync_date", fake_get)
    monkeypatch.setattr(sched, "record_sync_success", fake_record)
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda *a, **k: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )

    # D1 (08-11): prev=08-10, 未记录 → 执行, 记录数据日 08-10
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1]
    assert last_recorded["date"] == date(2026, 8, 10)

    # D2 (08-12): prev=08-11, last(08-10) < prev → 必须执行, 记录 08-11
    _FakeDate.current = date(2026, 8, 12)
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1, 1]
    assert last_recorded["date"] == date(2026, 8, 11)


def test_retryable_then_success_records_prev_day(monkeypatch):
    """重试成功后同样记录数据日 (前一天)"""
    statuses = iter([SYNC_ALL_RETRYABLE, SYNC_ALL_SUCCESS])
    calls = []

    def fake_flow(*args, **kwargs):
        calls.append(1)
        return next(statuses)

    monkeypatch.setattr(sched.sync_main, "sync_all_data_flow", fake_flow)
    recorded = []
    monkeypatch.setattr(
        sched, "record_sync_success", lambda d, s, dt: recorded.append((d, s, dt))
    )
    assert sched.run_sync_all_with_retry() == 0
    assert len(calls) == 2
    expected = (
        DATASET_SYNC_ALL,
        SYMBOL_SYNC_ALL,
        date.today() - sched.timedelta(days=1),
    )
    assert recorded == [expected]


def test_retry_exhausted_not_recorded(monkeypatch):
    """始终未全部成功时达最大重试次数即退出, 不记录状态"""
    calls = []

    def fake_flow(*args, **kwargs):
        calls.append(1)
        return SYNC_ALL_RETRYABLE

    monkeypatch.setattr(sched.sync_main, "sync_all_data_flow", fake_flow)
    recorded = []
    monkeypatch.setattr(sched, "record_sync_success", lambda *a: recorded.append(1))
    assert sched.run_sync_all_with_retry() == 1
    assert len(calls) == 3  # 1 + SYNC_ALL_MAX_RETRIES(2)
    assert recorded == []


def test_blocked_no_retry(monkeypatch):
    """新浪风控中止时不重试, 不记录状态"""
    calls = []

    def fake_flow(*args, **kwargs):
        calls.append(1)
        return SYNC_ALL_BLOCKED

    monkeypatch.setattr(sched.sync_main, "sync_all_data_flow", fake_flow)
    recorded = []
    monkeypatch.setattr(sched, "record_sync_success", lambda *a: recorded.append(1))
    assert sched.run_sync_all_with_retry() == 1
    assert len(calls) == 1
    assert recorded == []


def test_exception_treated_as_failure_and_retried(monkeypatch):
    """流水线抛异常视为未成功, 重试直至耗尽且不记录状态"""
    calls = []

    def fake_flow(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("模拟网络异常")
        return SYNC_ALL_RETRYABLE

    monkeypatch.setattr(sched.sync_main, "sync_all_data_flow", fake_flow)
    recorded = []
    monkeypatch.setattr(sched, "record_sync_success", lambda *a: recorded.append(1))
    assert sched.run_sync_all_with_retry() == 1
    assert len(calls) == 3
    assert recorded == []
