"""单元测试: tools/schedule_sync_all.py 定时调度入口 (mock 全部外部依赖)"""

import types
from datetime import date, timedelta

import pytest

import tools.schedule_sync_all as sched
from main import SYNC_ALL_BLOCKED, SYNC_ALL_RETRYABLE, SYNC_ALL_SUCCESS
from storage.database.sync_status import DATASET_SYNC_ALL, SYMBOL_SYNC_ALL

# 模拟周六凌晨触发时 get_latest_trade_date() 的返回值: 上周五 (周五休市场景单独覆盖)
DATA_DAY = date(2026, 10, 9)
# 周末(周六/周日)与节假日(周五休市)两种数据日下的"上一轮"记录日期
LAST_WEEK_DATA_DAY = DATA_DAY - timedelta(days=7)
FRIDAY_HOLIDAY_DATA_DAY = date(2026, 10, 8)  # 周四: 周五为节假日休市


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """默认环境: 固定重试参数、禁用真实 sleep、卷已挂载、最近交易日未记录成功"""
    monkeypatch.setattr(sched, "SYNC_ALL_MAX_RETRIES", 2)
    monkeypatch.setattr(sched, "SYNC_STATUS_MAX_RETRIES", 2)
    monkeypatch.setattr(sched, "SYNC_ALL_RETRY_INTERVAL_SECONDS", 0)
    # 仅替换 sched 命名空间内的 time 引用, 不影响全局 time 模块
    monkeypatch.setattr(sched, "time", types.SimpleNamespace(sleep=lambda _: None))
    monkeypatch.setattr(sched, "get_latest_trade_date", lambda: DATA_DAY)
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


def test_friday_holiday_uses_previous_trade_day(monkeypatch):
    """周五为节假日休市: 数据日取节前最后交易日 (周四), 仍执行并记录该日

    周调度下若沿用 today-1 判定会整周退出, 导致该周数据延迟两周 (回归防护)。
    """
    monkeypatch.setattr(sched, "get_latest_trade_date", lambda: FRIDAY_HOLIDAY_DATA_DAY)
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    recorded = []
    monkeypatch.setattr(
        sched, "record_sync_success", lambda d, s, dt: recorded.append((d, s, dt))
    )
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1]
    assert recorded == [(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL, FRIDAY_HOLIDAY_DATA_DAY)]


def test_trade_calendar_unavailable_is_failure(monkeypatch):
    from utils.trade_date import TradeCalendarUnavailableError

    def raise_calendar_error():
        raise TradeCalendarUnavailableError("无缓存")

    monkeypatch.setattr(sched, "get_latest_trade_date", raise_calendar_error)
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    assert sched.run_sync_all_with_retry() == 1
    assert flow_calls == []


def test_already_synced_data_day_exits(monkeypatch):
    """记录日期 >= 数据日 (最近交易日) → 跳过, 零同步请求"""
    monkeypatch.setattr(sched, "get_last_sync_date", lambda d, s: DATA_DAY)
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


def test_after_friday_holiday_week_next_saturday_runs(monkeypatch):
    """周五休市周之后的下一个周六: 数据日推进到新周五, 必须继续执行

    覆盖数据日不固定的场景: D1 因周五休市记录周四, D2 数据日为新周五,
    last(周四) < data_day(周五) → 正常执行, 不会因数据日跳变而永久跳过。
    """
    data_days = iter([FRIDAY_HOLIDAY_DATA_DAY, DATA_DAY + timedelta(days=7)])
    monkeypatch.setattr(sched, "get_latest_trade_date", lambda: next(data_days))

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

    # D1 (周五休市的那个周六): data_day=10-08 (周四), 未记录 → 执行, 记录 10-08
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1]
    assert last_recorded["date"] == FRIDAY_HOLIDAY_DATA_DAY

    # D2 (下周六): data_day=10-16 (周五), last(10-08) < data_day → 必须执行
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1, 1]
    assert last_recorded["date"] == DATA_DAY + timedelta(days=7)


def test_stale_sync_record_triggers_run(monkeypatch):
    """记录日期早于数据日 (最近交易日未同步成功) 时执行流水线"""
    monkeypatch.setattr(
        sched,
        "get_last_sync_date",
        lambda d, s: LAST_WEEK_DATA_DAY,
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


def test_manual_run_outside_saturday_syncs_latest_data_day(monkeypatch):
    """非周六 (如失败次日/周中) 手动执行: 数据日仍是最近交易日, 同样补跑

    注入周三数据日以区别于默认的周五触发场景: 验证 scheduler 的判定不依赖
    星期几, 失败后任意一天手动执行都能推进账本并补齐缺失交易日。
    """
    mid_week_data_day = DATA_DAY - timedelta(days=2)  # 周三
    monkeypatch.setattr(sched, "get_latest_trade_date", lambda: mid_week_data_day)
    flow_calls = []
    monkeypatch.setattr(
        sched.sync_main,
        "sync_all_data_flow",
        lambda: flow_calls.append(1) or SYNC_ALL_SUCCESS,
    )
    recorded = []
    monkeypatch.setattr(
        sched, "record_sync_success", lambda d, s, dt: recorded.append((d, s, dt))
    )
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1]
    assert recorded == [(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL, mid_week_data_day)]


def test_success_records_data_day(monkeypatch):
    """流水线成功时应记录数据日 (最近交易日), 保证下一次调度的判定基线推进"""
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
    assert recorded == [(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL, DATA_DAY)]
    assert installed == [1]


def test_consecutive_weeks_no_data_lag(monkeypatch):
    """跨两周连续触发: 记录数据日使下一周正常执行, 每周补齐全部缺失 (回归防护)

    旧实现记录运行日: D1 记录周六后, D2 判定 data_day == last 会错误跳过,
    导致整周数据延迟一周。本测试显式覆盖该场景: D2 必须执行流水线且记录
    新的数据日。
    """
    data_day_by_week = [DATA_DAY, LAST_WEEK_DATA_DAY + timedelta(days=14)]
    data_day_iter = iter(data_day_by_week)
    monkeypatch.setattr(sched, "get_latest_trade_date", lambda: next(data_day_iter))

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

    # D1 (周六): data_day=10-09, 未记录 → 执行, 记录数据日 10-09
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1]
    assert last_recorded["date"] == DATA_DAY

    # D2 (下周六): data_day=10-16, last(10-09) < data_day → 必须执行, 记录 10-16
    assert sched.run_sync_all_with_retry() == 0
    assert flow_calls == [1, 1]
    assert last_recorded["date"] == LAST_WEEK_DATA_DAY + timedelta(days=14)


def test_retryable_then_success_records_data_day(monkeypatch):
    """重试成功后同样记录数据日 (最近交易日)"""
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
    assert recorded == [(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL, DATA_DAY)]


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
