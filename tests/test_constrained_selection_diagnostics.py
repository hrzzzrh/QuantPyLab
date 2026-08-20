import json
import sys
from datetime import date

import pandas as pd
import pytest

import main as main_module
from backtest.config import BacktestConfig
from backtest.constrained_selection_diagnostics import (
    CONSTRAINT_SUMMARY_COLUMNS,
    _build_constrained_targets,
    allocate_proportional_quotas,
    calculate_constrained_selection_diagnostics,
    write_constrained_selection_diagnostic_report,
)
from backtest.data_access import BacktestDataAccess


def _candidates():
    dates = pd.to_datetime(["2024-01-31"] * 6 + ["2024-02-29"] * 6)
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [f"00000{index}" for index in range(1, 7)] * 2,
            "score": [
                0.9,
                0.8,
                0.7,
                0.6,
                0.5,
                0.4,
                0.85,
                0.75,
                0.65,
                0.55,
                0.45,
                0.35,
            ],
            "industry_code": (["480101"] * 3 + ["480301"] * 3) * 2,
            "market_cap": [1.0, 2.0, 3.0, 100.0, 200.0, 300.0] * 2,
        }
    )


def _baseline_targets():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 3 + ["2024-02-29"] * 3),
            "symbol": ["000001", "000002", "000003"] * 2,
            "score": [0.9, 0.8, 0.7, 0.85, 0.75, 0.65],
            "rank": [1, 2, 3] * 2,
            "target_weight": [1 / 3] * 6,
        }
    )


def test_allocate_proportional_quotas_uses_deterministic_hamilton_rounding():
    counts = pd.Series({"480101": 3, "480301": 3})

    assert allocate_proportional_quotas(counts, 3) == {
        "480101": 2,
        "480301": 1,
    }
    assert allocate_proportional_quotas(counts, 6) == {
        "480101": 3,
        "480301": 3,
    }


def test_allocate_proportional_quotas_respects_group_capacity():
    counts = pd.Series({"a": 1, "b": 5})

    quotas = allocate_proportional_quotas(counts, 4)

    assert quotas == {"a": 1, "b": 3}
    assert sum(quotas.values()) == 4
    assert all(quotas[key] <= counts[key] for key in quotas)


def test_allocate_proportional_quotas_rejects_invalid_inputs():
    with pytest.raises(ValueError, match="至少需要"):
        allocate_proportional_quotas(pd.Series(dtype=int), 2)
    with pytest.raises(ValueError, match="正整数"):
        allocate_proportional_quotas(pd.Series({"a": 2.5}), 2)
    with pytest.raises(ValueError, match="不能少于"):
        allocate_proportional_quotas(pd.Series({"a": 1}), 2)


@pytest.mark.parametrize(
    "mode",
    ["industry_quota", "size_quota", "industry_size_quota"],
)
def test_constrained_modes_keep_group_order_and_quota_error_zero(mode):
    targets, coverage, exposure = _build_constrained_targets(
        _candidates(),
        mode,
        holding_count=3,
        quantile_count=2,
    )

    first_date_targets = set(targets.loc[targets["date"] == "2024-01-31", "symbol"])
    assert first_date_targets == {"000001", "000002", "000004"}
    assert {row["status"] for row in coverage} == {"success"}
    assert exposure["quota_error"].abs().max() == 0


def test_constrained_size_buckets_are_stable_under_input_reordering():
    candidates = _candidates().copy()
    candidates.loc[candidates["symbol"].isin(["000003", "000004"]), "market_cap"] = 10.0
    reordered = candidates.iloc[[5, 3, 1, 4, 0, 2, 11, 9, 7, 10, 6, 8]]

    original_targets, _, _ = _build_constrained_targets(
        candidates,
        "size_quota",
        holding_count=3,
        quantile_count=2,
    )
    reordered_targets, _, _ = _build_constrained_targets(
        reordered,
        "size_quota",
        holding_count=3,
        quantile_count=2,
    )

    pd.testing.assert_frame_equal(
        original_targets.reset_index(drop=True),
        reordered_targets.reset_index(drop=True),
    )


def test_constrained_report_matches_baseline_and_writes_audit_files(tmp_path):
    report = calculate_constrained_selection_diagnostics(
        _candidates(),
        _baseline_targets(),
        holding_count=3,
        quantile_count=2,
    )

    assert list(report.summary.columns) == list(CONSTRAINT_SUMMARY_COLUMNS)
    assert report.summary["mode"].tolist() == [
        "baseline",
        "industry_quota",
        "size_quota",
        "industry_size_quota",
    ]
    for mode in ("industry_quota", "size_quota", "industry_size_quota"):
        overlap = report.target_overlap[report.target_overlap["mode"] == mode]
        assert overlap["overlap_rate"].tolist() == pytest.approx([2 / 3, 2 / 3])
    assert report.exposure["quota_error"].abs().max() == 0

    output_dir = write_constrained_selection_diagnostic_report(
        report,
        tmp_path / "constraints",
        parameters={"quota_method": "Hamilton largest remainder"},
    )
    for filename in (
        "summary.md",
        "parameters.json",
        "constraint_summary.csv",
        "constraint_coverage.csv",
        "constraint_target_overlap.csv",
        "constraint_exposure.csv",
    ):
        assert output_dir.joinpath(filename).exists()
    payload = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert payload["quota_method"] == "Hamilton largest remainder"
    with pytest.raises(FileExistsError):
        write_constrained_selection_diagnostic_report(report, output_dir, parameters={})


def test_constrained_joint_mode_does_not_fallback_with_missing_controls():
    candidates = _candidates().iloc[:6].copy()
    candidates.loc[0, "industry_code"] = None
    candidates.loc[1, "market_cap"] = 0

    report = calculate_constrained_selection_diagnostics(
        candidates,
        _baseline_targets().iloc[:3],
        holding_count=5,
        quantile_count=2,
    )

    joint_coverage = report.coverage[report.coverage["mode"] == "industry_size_quota"]
    assert joint_coverage.iloc[0]["valid_control_count"] == 4
    assert joint_coverage.iloc[0]["status"] == "failed"
    joint_overlap = report.target_overlap[
        report.target_overlap["mode"] == "industry_size_quota"
    ]
    assert joint_overlap.iloc[0]["constrained_target_count"] == 0
    assert pd.isna(joint_overlap.iloc[0]["overlap_rate"])


def test_constrained_joint_mode_can_use_available_size_buckets():
    candidates = _candidates().iloc[:6].copy()
    candidates.loc[
        candidates["symbol"].isin(["000004", "000005", "000006"]), "industry_code"
    ] = None

    report = calculate_constrained_selection_diagnostics(
        candidates,
        _baseline_targets().iloc[:3],
        holding_count=3,
        quantile_count=2,
    )

    joint_coverage = report.coverage[report.coverage["mode"] == "industry_size_quota"]
    assert joint_coverage.iloc[0]["valid_control_count"] == 3
    assert joint_coverage.iloc[0]["status"] == "success"
    assert joint_coverage.iloc[0]["selected_count"] == 3


def test_run_factor_constrained_selection_diagnostics_writes_real_shape(
    monkeypatch, tmp_path
):
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 1),
        strategy_name="factor-composite-experiment",
        strategy_parameters={
            "factor_weights": {"valuation_pb": 1.0},
            "holding_count": 3,
            "min_listing_days": 1,
        },
        benchmark_symbol=None,
    )
    signal_data = _candidates().assign(
        open_hfq=100.0,
        close_hfq=101.0,
        pb=[6.0, 5.0, 4.0, 3.0, 2.0, 1.0] * 2,
    )
    monkeypatch.setattr("backtest.config.load_backtest_config", lambda _: config)
    monkeypatch.setattr(
        BacktestDataAccess,
        "load_factor_data",
        lambda self, *args, **kwargs: signal_data,
    )
    monkeypatch.setattr(
        BacktestDataAccess,
        "load_point_in_time_industry",
        lambda self, points: points.assign(industry_code="480101"),
    )
    monkeypatch.setattr(
        BacktestDataAccess,
        "get_industry_snapshot_metadata",
        lambda self: {"snapshot_sha256": "c" * 64},
    )

    output_dir = main_module.run_factor_constrained_selection_diagnostics(
        "candidate.toml",
        quantile_count=2,
        output_path=str(tmp_path / "cli-output"),
    )

    assert output_dir.joinpath("summary.md").exists()
    assert len(pd.read_csv(output_dir / "constraint_summary.csv")) == 4
    parameters = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert parameters["quota_method"] == "Hamilton largest remainder"


def test_run_factor_constrained_selection_rejects_invalid_quantile_before_load(
    monkeypatch,
):
    monkeypatch.setattr(
        "backtest.config.load_backtest_config",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("非法 quantile_count 不应触发数据加载")
        ),
    )

    with pytest.raises(ValueError, match="quantile_count"):
        main_module.run_factor_constrained_selection_diagnostics(
            "candidate.toml",
            quantile_count=0,
        )


def test_run_factor_constrained_selection_rejects_other_strategy(monkeypatch):
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 1),
        strategy_name="price-momentum",
        benchmark_symbol=None,
    )
    monkeypatch.setattr("backtest.config.load_backtest_config", lambda _: config)

    with pytest.raises(ValueError, match="只支持 factor-composite-experiment"):
        main_module.run_factor_constrained_selection_diagnostics("candidate.toml")


def test_cli_constrained_selection_exception_exits_1(monkeypatch):
    import utils.requests_protection as requests_protection

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "diagnose-factor-constrained-selection",
            "--backtest-config",
            "candidate.toml",
        ],
    )
    monkeypatch.setattr(
        requests_protection, "install_requests_protection", lambda: None
    )

    def fail(**_kwargs):
        raise ValueError("测试错误路径")

    monkeypatch.setattr(
        main_module,
        "run_factor_constrained_selection_diagnostics",
        fail,
    )
    with pytest.raises(SystemExit) as exc:
        main_module.main()
    assert exc.value.code == 1
