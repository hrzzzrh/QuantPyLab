"""单元测试: main.py sync_all_data_flow 三态判定 (mock 全部环节, 不触网络与数据库)"""

import sys

import pytest

import main as main_mod
from main import (
    SYNC_ALL_BLOCKED,
    SYNC_ALL_RETRYABLE,
    SYNC_ALL_SUCCESS,
    sync_all_data_flow,
)
from utils.requests_protection import SinaBlockedError

FULL_STAGES = {
    "sync_stock_list": (1, 0),
    "sync_stock_metadata": (10, 0),
    "sync_financial_indicators": (5, 0),
    "sync_financial_statements": (5, 0),
    "calculate_ttm_metrics": (100, 0),
    "sync_share_capital": (5000, 0),
    "sync_daily_kline": (5000, 0),
}


def _install_stages(monkeypatch, stats: dict) -> None:
    """把各环节替换为返回给定 (processed, failed) 的桩函数"""
    for name, (processed, failed) in stats.items():
        monkeypatch.setattr(
            main_mod,
            name,
            lambda *args, _p=processed, _f=failed, **kwargs: (_p, _f),
        )


def test_all_stages_success_returns_success(monkeypatch):
    _install_stages(monkeypatch, FULL_STAGES)
    assert sync_all_data_flow() == SYNC_ALL_SUCCESS


def test_single_stage_failure_returns_retryable(monkeypatch):
    stats = dict(FULL_STAGES)
    stats["sync_daily_kline"] = (5000, 3)
    _install_stages(monkeypatch, stats)
    assert sync_all_data_flow() == SYNC_ALL_RETRYABLE


def test_metadata_failure_returns_retryable(monkeypatch):
    """元数据环节失败同样计入整体失败判定 (7 环节全覆盖)"""
    stats = dict(FULL_STAGES)
    stats["sync_stock_metadata"] = (10, 2)
    _install_stages(monkeypatch, stats)
    assert sync_all_data_flow() == SYNC_ALL_RETRYABLE


def test_sina_blocked_returns_blocked(monkeypatch):
    stats = dict(FULL_STAGES)
    stats.pop("sync_stock_list")
    _install_stages(monkeypatch, stats)

    def boom(*args, **kwargs):
        raise SinaBlockedError("IP 风控测试")

    monkeypatch.setattr(main_mod, "sync_stock_list", boom)
    assert sync_all_data_flow() == SYNC_ALL_BLOCKED


def test_sina_blocked_from_kline_returns_blocked(monkeypatch):
    """kline/share 环节传播的风控同样判定 BLOCKED (不被计数吞掉)"""
    _install_stages(monkeypatch, FULL_STAGES)

    def boom(*args, **kwargs):
        raise SinaBlockedError("K线环节 IP 风控")

    monkeypatch.setattr(main_mod, "sync_daily_kline", boom)
    assert sync_all_data_flow() == SYNC_ALL_BLOCKED


def test_single_symbol_skips_list_and_metadata(monkeypatch):
    """单股模式不执行名单与元数据环节, 其余环节正常执行"""
    called = []

    def record_call(name):
        def _stage(*args, **kwargs):
            called.append(name)
            return 0, 0

        return _stage

    for name in FULL_STAGES:
        monkeypatch.setattr(main_mod, name, record_call(name))
    assert sync_all_data_flow(symbol="600519") == SYNC_ALL_SUCCESS
    assert "sync_stock_list" not in called
    assert "sync_stock_metadata" not in called
    assert "sync_daily_kline" in called


def test_cli_sync_all_blocked_exits_1(monkeypatch):
    """CLI sync-all 遇风控中止应退出码 1 (而非静默成功)"""
    import utils.requests_protection as rp_mod

    monkeypatch.setattr(sys, "argv", ["main.py", "sync-all"])
    monkeypatch.setattr(rp_mod, "install_requests_protection", lambda: None)
    monkeypatch.setattr(main_mod, "sync_all_data_flow", lambda **kw: SYNC_ALL_BLOCKED)
    with pytest.raises(SystemExit) as exc:
        main_mod.main()
    assert exc.value.code == 1


def test_cli_sync_all_retryable_exits_1(monkeypatch):
    """CLI sync-all 存在环节失败时应返回非零退出码"""
    import utils.requests_protection as rp_mod

    monkeypatch.setattr(sys, "argv", ["main.py", "sync-all"])
    monkeypatch.setattr(rp_mod, "install_requests_protection", lambda: None)
    monkeypatch.setattr(main_mod, "sync_all_data_flow", lambda **kw: SYNC_ALL_RETRYABLE)
    with pytest.raises(SystemExit) as exc:
        main_mod.main()
    assert exc.value.code == 1


def test_cli_sync_all_exception_exits_1(monkeypatch):
    """CLI sync-all 普通异常也必须返回非零退出码"""
    import utils.requests_protection as rp_mod

    monkeypatch.setattr(sys, "argv", ["main.py", "sync-all"])
    monkeypatch.setattr(rp_mod, "install_requests_protection", lambda: None)
    monkeypatch.setattr(
        main_mod,
        "sync_all_data_flow",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("流水线异常")),
    )
    with pytest.raises(SystemExit) as exc:
        main_mod.main()
    assert exc.value.code == 1
