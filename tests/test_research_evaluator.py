import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import backtest.research_evaluator as evaluator_module
import main as main_module
from backtest.config import BacktestConfig, load_backtest_config
from backtest.engine import BacktestResult
from backtest.factor_trainer import FactorTrainingResult
from backtest.hyperparameter_search import (
    HyperparameterSearchSpec,
    expand_hyperparameter_trials,
)
from backtest.research_evaluator import (
    EvaluationPeriod,
    EvaluationSplit,
    FactorExperimentEvaluationConfig,
    FactorExperimentEvaluationResult,
    ResearchValiditySpec,
    TrainingSpec,
    WalkForwardSpec,
    build_walk_forward_splits,
    evaluate_factor_experiments,
    load_factor_experiment_evaluation_config,
    write_factor_experiment_evaluation_report,
)
from backtest.runner import BacktestRun


def test_walk_forward_generates_non_overlapping_rolling_windows():
    windows = build_walk_forward_splits(
        WalkForwardSpec(
            start_date=date(2020, 1, 1),
            end_date=date(2025, 12, 31),
            train_years=2,
            validation_years=1,
            test_years=1,
            step_years=1,
        )
    )

    assert len(windows) == 3
    assert windows[0].train == EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31))
    assert windows[0].validation == EvaluationPeriod(
        date(2022, 1, 1), date(2022, 12, 31)
    )
    assert windows[0].test == EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31))
    assert windows[-1].test.end_date == date(2025, 12, 31)


def test_loads_fixed_and_walk_forward_evaluation_config():
    config = load_factor_experiment_evaluation_config(
        "config/backtest/factor_experiment_evaluation.toml"
    )

    assert config.selection_metric == "sharpe_ratio"
    assert config.selection_direction == "max"
    assert len(config.candidate_configs) == 2
    assert len(config.get_splits()) == 5
    assert config.training is not None
    assert config.training.label_horizon_days == 20
    assert config.training.minimum_training_dates == 24
    assert config.validity is not None
    assert config.validity.minimum_validation_signal_dates == 11
    assert config.hyperparameter_search is not None
    assert config.hyperparameter_search.holding_counts == (10, 20, 50)
    audit = evaluator_module._build_reproducibility_audit(config)
    coverage = audit["search_parameter_coverage"]
    assert coverage["factor_experiment_reversal"]["applied_factor_parameters"] == [
        "price_reversal_20d"
    ]
    assert coverage["factor_experiment_value_growth"]["ignored_factor_parameters"] == [
        "price_reversal_20d"
    ]


def test_loads_robust_evaluation_config_with_longer_time_windows():
    config = load_factor_experiment_evaluation_config(
        "config/backtest/factor_experiment_evaluation_robust.toml"
    )

    assert len(config.get_splits()) == 10
    assert config.fixed_split.train == EvaluationPeriod(
        date(2017, 7, 1), date(2020, 6, 30)
    )
    assert config.fixed_split.validation == EvaluationPeriod(
        date(2020, 7, 1), date(2022, 6, 30)
    )
    assert config.fixed_split.test == EvaluationPeriod(
        date(2022, 7, 1), date(2024, 6, 30)
    )
    assert config.training is not None
    assert config.training.minimum_training_observations == 200
    assert config.training.minimum_training_dates == 24
    assert config.validity is not None
    assert config.validity.minimum_training_signal_dates == 24
    assert config.validity.minimum_validation_signal_dates == 20
    assert config.validity.minimum_test_signal_dates == 20
    assert config.validity.minimum_validation_observations == 100
    assert config.validity.minimum_test_observations == 100
    assert config.hyperparameter_search is not None
    assert config.hyperparameter_search.ridge_alphas == (0.1,)
    assert config.walk_forward is not None
    assert config.walk_forward.start_date == date(2011, 1, 1)
    assert config.walk_forward.end_date == date(2025, 12, 31)
    assert config.walk_forward.train_years == 3
    assert config.walk_forward.validation_years == 2
    assert config.walk_forward.test_years == 2
    assert config.walk_forward.step_years == 1
    candidate_configs = [
        (path.stem, load_backtest_config(path)) for path in config.candidate_configs
    ]
    assert (
        len(
            expand_hyperparameter_trials(
                candidate_configs, config.hyperparameter_search
            )
        )
        == 18
    )


def test_research_validity_defaults_when_section_is_omitted(tmp_path):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    config_path = tmp_path / "evaluation.toml"
    config_path.write_text(
        """
[experiment]
name = "default-validity"
candidate_configs = ["candidate.toml"]

[split]
train_start_date = "2020-01-01"
train_end_date = "2020-12-31"
validation_start_date = "2021-01-01"
validation_end_date = "2021-12-31"
test_start_date = "2022-01-01"
test_end_date = "2022-12-31"
""",
        encoding="utf-8",
    )

    config = load_factor_experiment_evaluation_config(config_path)

    assert config.validity is not None
    assert config.validity.enabled is True
    assert config.validity.minimum_training_signal_dates == 24


def test_research_validity_gate_blocks_short_validation_window(monkeypatch, tmp_path):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="validity-gate",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        validity=ResearchValiditySpec(
            minimum_training_signal_dates=1,
            minimum_validation_signal_dates=2,
            minimum_test_signal_dates=2,
            minimum_validation_observations=2,
            minimum_test_observations=2,
        ),
    )
    calls = []

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name=path.stem,
            benchmark_symbol=None,
        )

    def fake_execute_backtest(
        candidate_config, database_manager, *, include_benchmark, execution_cache=None
    ):
        calls.append(candidate_config.start_date)
        signal_date = (
            pd.Timestamp("2020-01-31")
            if candidate_config.start_date == split.train.start_date
            else pd.Timestamp("2021-01-31")
        )
        daily_nav = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-02"), "nav": 1.0},
                {"date": pd.Timestamp("2024-01-03"), "nav": 1.01},
            ]
        )
        targets = pd.DataFrame(
            {
                "date": [signal_date],
                "symbol": ["000001"],
            }
        )
        return BacktestRun(
            config=candidate_config,
            targets=targets,
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute_backtest)
    monkeypatch.setattr(
        evaluator_module,
        "calculate_performance_metrics",
        lambda daily_nav: {
            "total_return": 0.01,
            "annualized_return": 0.01,
            "annualized_volatility": 0.1,
            "sharpe_ratio": 1.0,
            "max_drawdown": -0.01,
            "trading_days": 2,
        },
    )

    result = evaluate_factor_experiments(config, object())

    assert calls == [split.train.start_date, split.validation.start_date]
    assert result.selection_rows[0]["status"] == "failed"
    assert result.evaluation_failure_rows[0]["phase"] == "validation"
    assert result.validity_rows[-1]["status"] == "failed"
    assert "研究有效性门禁失败" in result.validity_rows[-1]["error"]


def test_research_validity_gate_blocks_short_training_fit(monkeypatch, tmp_path):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="training-validity-gate",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=1,
            minimum_training_dates=1,
        ),
        validity=ResearchValiditySpec(
            minimum_training_signal_dates=2,
            minimum_validation_signal_dates=1,
            minimum_test_signal_dates=1,
            minimum_validation_observations=1,
            minimum_test_observations=1,
        ),
    )

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name="factor-composite-experiment",
            strategy_parameters={"factor_weights": {"valuation_pb": 1.0}},
            benchmark_symbol=None,
        )

    def fake_fit(candidate_config, train_period, training, database_manager):
        return candidate_config, FactorTrainingResult(
            factor_weights={"valuation_pb": 1.0},
            observation_count=2,
            signal_date_count=1,
            iterations=2,
            converged=True,
            label_horizon_days=20,
            ridge_alpha=0.1,
        )

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "_fit_candidate_config", fake_fit)

    result = evaluate_factor_experiments(config, object())

    assert result.selection_rows[0]["status"] == "failed"
    assert result.training_rows[0]["status"] == "failed"
    assert result.training_rows[0]["signal_date_count"] == 1
    assert result.validity_rows[0]["phase"] == "training"
    assert result.validity_rows[0]["status"] == "failed"
    assert "minimum_signal_dates=2" in result.validity_rows[0]["error"]


def test_research_validity_report_contains_phase_coverage(tmp_path):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="validity-report",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        validity=ResearchValiditySpec(
            minimum_training_signal_dates=1,
            minimum_validation_signal_dates=1,
            minimum_test_signal_dates=1,
            minimum_validation_observations=1,
            minimum_test_observations=1,
        ),
    )
    result = FactorExperimentEvaluationResult(
        config=config,
        metric_rows=(
            {
                "split_id": "fixed_split",
                "phase": "validation",
                "candidate_id": "candidate",
                "sharpe_ratio": 1.0,
                "target_observation_count": 2,
                "signal_date_count": 1,
            },
            {
                "split_id": "fixed_split",
                "phase": "test",
                "candidate_id": "candidate",
                "sharpe_ratio": 1.0,
                "target_observation_count": 2,
                "signal_date_count": 1,
            },
        ),
        selection_rows=(),
        validity_rows=(
            {
                "split_id": "fixed_split",
                "candidate_id": "candidate",
                "trial_id": "candidate",
                "phase": "validation",
                "status": "passed",
                "error": "",
                "observation_count": 2,
                "signal_date_count": 1,
                "minimum_observations": 1,
                "minimum_signal_dates": 1,
            },
        ),
    )

    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "validity-report-output"
    )

    assert output_dir.joinpath("research_validity.csv").exists()
    report = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "研究有效性门禁" in report
    assert "research_validity.csv" in report


def test_target_coverage_excludes_signal_without_t_plus_one_execution():
    period = EvaluationPeriod(date(2021, 1, 1), date(2021, 2, 28))
    targets = pd.DataFrame(
        {
            "date": [
                pd.Timestamp("2021-01-29"),
                pd.Timestamp("2021-02-26"),
            ],
            "symbol": ["000001", "000002"],
        }
    )
    daily_nav = pd.DataFrame(
        {
            "date": [
                pd.Timestamp("2021-01-29"),
                pd.Timestamp("2021-02-26"),
            ],
            "nav": [1.0, 1.01],
        }
    )

    coverage = evaluator_module._summarize_target_coverage(
        targets,
        period,
        daily_nav,
    )

    assert coverage["target_observation_count"] == 1
    assert coverage["signal_date_count"] == 1
    assert coverage["signal_date_start"] == "2021-01-29"
    assert coverage["signal_date_end"] == "2021-01-29"


def test_research_evaluator_runs_test_only_for_validation_winner(monkeypatch, tmp_path):
    candidate_a = tmp_path / "candidate_a.toml"
    candidate_b = tmp_path / "candidate_b.toml"
    candidate_a.write_text("", encoding="utf-8")
    candidate_b.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31)),
        validation=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
        test=EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="test-evaluation",
        candidate_configs=(candidate_a, candidate_b),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
    )
    calls = []

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name=path.stem,
            benchmark_symbol=None,
        )

    def fake_execute_backtest(
        candidate_config, database_manager, *, include_benchmark, execution_cache=None
    ):
        assert include_benchmark is False
        phase = (
            "validation"
            if candidate_config.start_date == split.validation.start_date
            else "test"
            if candidate_config.start_date == split.test.start_date
            else "train"
        )
        calls.append((candidate_config.strategy_name, phase))
        score = {
            ("candidate_a", "train"): 1.0,
            ("candidate_a", "validation"): 1.0,
            ("candidate_a", "test"): 99.0,
            ("candidate_b", "train"): 2.0,
            ("candidate_b", "validation"): 2.0,
            ("candidate_b", "test"): 3.0,
        }[(candidate_config.strategy_name, phase)]
        daily_nav = pd.DataFrame([{"date": pd.Timestamp("2024-01-02"), "nav": 1.0}])
        daily_nav.attrs["score"] = score
        return BacktestRun(
            config=candidate_config,
            targets=pd.DataFrame(),
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    def fake_metrics(daily_nav):
        score = daily_nav.attrs["score"]
        return {
            "total_return": score,
            "annualized_return": score,
            "annualized_volatility": 1.0,
            "sharpe_ratio": score,
            "max_drawdown": -score,
            "trading_days": 1,
        }

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute_backtest)
    monkeypatch.setattr(evaluator_module, "calculate_performance_metrics", fake_metrics)

    result = evaluate_factor_experiments(config, object())

    assert result.selection_rows[0]["selected_candidate"] == "candidate_b"
    assert result.selection_rows[0]["validation_score"] == 2.0
    assert calls == [
        ("candidate_a", "train"),
        ("candidate_a", "validation"),
        ("candidate_b", "train"),
        ("candidate_b", "validation"),
        ("candidate_b", "test"),
    ]
    assert len(result.metric_rows) == 5

    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "evaluation-output"
    )
    assert output_dir.joinpath("parameters.json").exists()
    assert output_dir.joinpath("candidate_metrics.csv").exists()
    assert output_dir.joinpath("selections.csv").exists()
    assert output_dir.joinpath("training_models.csv").exists()
    assert output_dir.joinpath("hyperparameter_trials.csv").exists()
    assert output_dir.joinpath("evaluation_failures.csv").exists()
    assert output_dir.joinpath("selection_diagnostics.csv").exists()
    assert output_dir.joinpath("factor_weight_diagnostics.csv").exists()
    diagnostics = pd.read_csv(output_dir / "selection_diagnostics.csv")
    assert list(diagnostics.columns) == list(
        evaluator_module.SELECTION_DIAGNOSTIC_COLUMNS
    )
    assert len(diagnostics) == 1
    diagnostic = diagnostics.iloc[0]
    assert diagnostic["candidate_count"] == 2
    assert diagnostic["fitted_trial_count"] == 2
    assert diagnostic["selected_candidate"] == "candidate_b"
    assert diagnostic["selected_rank"] == 1
    assert diagnostic["selection_score_margin"] == pytest.approx(1.0)
    parameters = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert "audit" in parameters
    assert parameters["audit"]["candidate_config_sha256"]
    report = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "# 因子训练标准结果报告" in report
    assert "未启用权重训练" in report
    assert "本次未启用权重训练，无法计算权重集中度。" in report
    assert "验证集选择稳健性" in report
    assert "## 9. 审计文件" in report
    assert "测试窗口只有 1 个有效窗口" in report


def test_selection_diagnostic_quantifies_maximization_search_burden():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    trial_map = {
        "trial_a": SimpleNamespace(candidate_id="candidate_a"),
        "trial_b": SimpleNamespace(candidate_id="candidate_b"),
        "trial_c": SimpleNamespace(candidate_id="candidate_b"),
    }

    diagnostic = evaluator_module._build_selection_diagnostic_row(
        split,
        {"trial_a": 0.8, "trial_b": 0.6, "trial_c": 0.4},
        trial_map,
        selected_trial_id="trial_a",
        selected_score=0.8,
        validation_coverage={
            "signal_date_count": 2,
            "target_observation_count": 20,
        },
        status="completed",
        error="",
        selection_direction="max",
    )

    assert diagnostic["candidate_count"] == 2
    assert diagnostic["fitted_trial_count"] == 3
    assert diagnostic["selected_rank"] == 1
    assert diagnostic["second_validation_score"] == 0.6
    assert diagnostic["selection_score_margin"] == pytest.approx(0.2)
    assert diagnostic["validation_score_median"] == 0.6
    assert diagnostic["trials_per_validation_signal_date"] == 1.5
    assert diagnostic["risk_level"] == "high"

    config = FactorExperimentEvaluationConfig(
        name="selection-warning",
        candidate_configs=(Path("candidate.toml"),),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
    )
    result = FactorExperimentEvaluationResult(
        config=config,
        metric_rows=(),
        selection_rows=(),
        selection_diagnostic_rows=(diagnostic,),
    )
    warnings = evaluator_module._build_report_warnings(result, [], [], [])
    assert any("多重比较负担" in warning for warning in warnings)
    assert any("验证集只有 2 个" in warning for warning in warnings)


def test_selection_diagnostic_handles_minimization_ties_and_failed_selection():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    trial_map = {
        "trial_a": SimpleNamespace(candidate_id="candidate_a"),
        "trial_b": SimpleNamespace(candidate_id="candidate_b"),
    }

    diagnostic = evaluator_module._build_selection_diagnostic_row(
        split,
        {"trial_a": 0.1, "trial_b": 0.1},
        trial_map,
        selected_trial_id="trial_a",
        selected_score=0.1,
        validation_coverage={"signal_date_count": 20},
        status="completed",
        error="",
        selection_direction="min",
    )
    failed = evaluator_module._build_selection_diagnostic_row(
        split,
        {},
        trial_map,
        selected_trial_id=None,
        selected_score=None,
        validation_coverage=None,
        status="failed",
        error="没有有效分数",
        selection_direction="min",
    )

    assert diagnostic["selected_rank"] == 1
    assert diagnostic["selection_score_margin"] == 0.0
    assert diagnostic["tied_best_trial_count"] == 2
    assert diagnostic["risk_level"] == "not_flagged"
    assert failed["status"] == "failed"
    assert failed["risk_level"] == "not_available"
    assert failed["selection_score_margin"] is None


def test_research_evaluator_fits_weights_before_running_validation(
    monkeypatch, tmp_path
):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31)),
        validation=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
        test=EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="trained-evaluation",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=2,
            minimum_training_dates=1,
        ),
    )
    calls = []

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name="factor-composite-experiment",
            strategy_parameters={
                "factor_weights": {"valuation_pb": 1.0},
            },
            benchmark_symbol=None,
        )

    def fake_fit(candidate_config, train_period, training, database_manager):
        calls.append(("fit", train_period))
        trained = candidate_config.__class__(
            **{
                **candidate_config.__dict__,
                "strategy_parameters": {
                    "factor_weights": {"valuation_pb": 0.8},
                },
            }
        )
        return trained, FactorTrainingResult(
            factor_weights={"valuation_pb": 0.8, "quality_roic": 0.2},
            observation_count=20,
            signal_date_count=5,
            iterations=4,
            converged=True,
            label_horizon_days=20,
            ridge_alpha=0.1,
        )

    def fake_execute_backtest(
        candidate_config, database_manager, *, include_benchmark, execution_cache=None
    ):
        assert include_benchmark is False
        calls.append(
            (
                "run",
                candidate_config.start_date,
                candidate_config.end_date,
                candidate_config.strategy_parameters,
            )
        )
        daily_nav = pd.DataFrame([{"date": pd.Timestamp("2024-01-02"), "nav": 1.0}])
        daily_nav.attrs["score"] = 1.0
        return BacktestRun(
            config=candidate_config,
            targets=pd.DataFrame(),
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "_fit_candidate_config", fake_fit)
    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute_backtest)
    monkeypatch.setattr(
        evaluator_module,
        "calculate_performance_metrics",
        lambda daily_nav: {
            "total_return": 1.0,
            "annualized_return": 1.0,
            "annualized_volatility": 1.0,
            "sharpe_ratio": 1.0,
            "max_drawdown": -1.0,
            "trading_days": 1,
        },
    )

    result = evaluate_factor_experiments(config, object())

    assert calls[0][0] == "fit"
    assert calls[1][0] == "run"
    assert calls[1][3]["factor_weights"] == {"valuation_pb": 0.8}
    assert len(result.training_rows) == 1
    assert result.training_rows[0]["factor_weights"] == (
        '{"quality_roic": 0.2, "valuation_pb": 0.8}'
    )
    assert result.selection_rows[0]["selected_holding_count"] == 20
    assert result.selection_rows[0]["selected_winsorize_lower"] == 0.05
    assert result.selection_rows[0]["selected_ridge_alpha"] == 0.1


def test_research_evaluator_excludes_candidate_with_failed_training(
    monkeypatch, tmp_path
):
    candidate_a = tmp_path / "candidate_a.toml"
    candidate_b = tmp_path / "candidate_b.toml"
    candidate_a.write_text("", encoding="utf-8")
    candidate_b.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31)),
        validation=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
        test=EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="failed-candidate-evaluation",
        candidate_configs=(candidate_a, candidate_b),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=2,
            minimum_training_dates=1,
        ),
    )
    fit_calls = []

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name="factor-composite-experiment",
            strategy_parameters={"factor_weights": {"valuation_pb": 1.0}},
            benchmark_symbol=None,
        )

    def fake_fit(candidate_config, train_period, training, database_manager):
        fit_calls.append(candidate_config)
        if len(fit_calls) == 1:
            raise ValueError("训练样本不足")
        return candidate_config, FactorTrainingResult(
            factor_weights={"valuation_pb": 1.0},
            observation_count=20,
            signal_date_count=5,
            iterations=2,
            converged=True,
            label_horizon_days=20,
            ridge_alpha=0.1,
        )

    def fake_execute_backtest(
        candidate_config, database_manager, *, include_benchmark, execution_cache=None
    ):
        daily_nav = pd.DataFrame([{"date": pd.Timestamp("2024-01-02"), "nav": 1.0}])
        daily_nav.attrs["score"] = 2.0
        return BacktestRun(
            config=candidate_config,
            targets=pd.DataFrame(),
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "_fit_candidate_config", fake_fit)
    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute_backtest)
    monkeypatch.setattr(
        evaluator_module,
        "calculate_performance_metrics",
        lambda daily_nav: {
            "total_return": 2.0,
            "annualized_return": 2.0,
            "annualized_volatility": 1.0,
            "sharpe_ratio": 2.0,
            "max_drawdown": -2.0,
            "trading_days": 1,
        },
    )

    result = evaluate_factor_experiments(config, object())

    assert len(fit_calls) == 2
    assert [row["status"] for row in result.training_rows] == ["failed", "fitted"]
    assert result.training_rows[0]["error"] == "训练样本不足"
    assert {row["candidate_id"] for row in result.metric_rows} == {"candidate_b"}
    assert result.selection_rows[0]["selected_candidate"] == "candidate_b"

    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "failed-candidate-output"
    )
    report = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "completed_with_exclusions" in report
    assert "训练样本不足" in report
    assert "被排除" in report


def test_research_evaluator_excludes_candidate_with_failed_backtest(
    monkeypatch,
):
    candidate_a = type("CandidatePath", (), {"stem": "candidate_a"})()
    candidate_b = type("CandidatePath", (), {"stem": "candidate_b"})()
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31)),
        validation=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
        test=EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="failed-backtest-evaluation",
        candidate_configs=(candidate_a, candidate_b),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
    )

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name=path.stem,
            benchmark_symbol=None,
        )

    def fake_execute_backtest(
        candidate_config, database_manager, *, include_benchmark, execution_cache=None
    ):
        if (
            candidate_config.strategy_name == "candidate_a"
            and candidate_config.start_date == split.validation.start_date
        ):
            raise RuntimeError("模拟回测失败")
        daily_nav = pd.DataFrame([{"date": pd.Timestamp("2024-01-02"), "nav": 1.0}])
        return BacktestRun(
            config=candidate_config,
            targets=pd.DataFrame(),
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute_backtest)
    monkeypatch.setattr(
        evaluator_module,
        "calculate_performance_metrics",
        lambda daily_nav: {
            "total_return": 0.1,
            "annualized_return": 0.1,
            "annualized_volatility": 0.1,
            "sharpe_ratio": 1.0,
            "max_drawdown": -0.1,
            "trading_days": 1,
        },
    )

    result = evaluate_factor_experiments(config, object())

    assert result.selection_rows[0]["selected_candidate"] == "candidate_b"
    assert len(result.evaluation_failure_rows) == 1
    assert {row["candidate_id"] for row in result.evaluation_failure_rows} == {
        "candidate_a"
    }


def test_research_evaluator_records_invalid_selection_metrics(
    monkeypatch,
    tmp_path,
):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31)),
        validation=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
        test=EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="invalid-metric-evaluation",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
    )

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name=path.stem,
            benchmark_symbol=None,
        )

    def fake_execute_backtest(
        candidate_config, database_manager, *, include_benchmark, execution_cache=None
    ):
        phase = (
            "validation"
            if candidate_config.start_date == split.validation.start_date
            else "test"
            if candidate_config.start_date == split.test.start_date
            else "train"
        )
        daily_nav = pd.DataFrame([{"date": pd.Timestamp("2024-01-02"), "nav": 1.0}])
        daily_nav.attrs["phase"] = phase
        return BacktestRun(
            config=candidate_config,
            targets=pd.DataFrame(),
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    def fake_metrics(daily_nav):
        return {
            "total_return": 0.1,
            "annualized_return": 0.1,
            "annualized_volatility": 0.1,
            "sharpe_ratio": None if daily_nav.attrs["phase"] == "validation" else 1.0,
            "max_drawdown": -0.1,
            "trading_days": 1,
        }

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute_backtest)
    monkeypatch.setattr(evaluator_module, "calculate_performance_metrics", fake_metrics)

    result = evaluate_factor_experiments(config, object())

    assert result.selection_rows[0]["status"] == "failed"
    assert len(result.evaluation_failure_rows) == 1
    assert result.evaluation_failure_rows[0]["phase"] == "validation"
    assert "sharpe_ratio" in result.evaluation_failure_rows[0]["error"]

    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "invalid-metric-output"
    )
    diagnostics = pd.read_csv(output_dir / "selection_diagnostics.csv")
    assert len(diagnostics) == 1
    diagnostic = diagnostics.iloc[0]
    assert diagnostic["status"] == "failed"
    assert "所有候选验证指标均为空" in diagnostic["error"]
    assert pd.isna(diagnostic["selected_trial_id"])


def test_research_evaluator_searches_hyperparameters_and_tests_one_trial(
    monkeypatch, tmp_path
):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31)),
        validation=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
        test=EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="hyperparameter-search",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=2,
            minimum_training_dates=1,
        ),
        hyperparameter_search=HyperparameterSearchSpec(
            max_combinations=8,
            holding_counts=(10, 20),
            winsorize_ranges=((0.05, 0.95), (0.1, 0.9)),
            ridge_alphas=(0.1, 1.0),
        ),
    )
    fit_calls = []
    run_calls = []

    def fake_load_backtest_config(path):
        return BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name="factor-composite-experiment",
            strategy_parameters={"factor_weights": {"valuation_pb": 1.0}},
            benchmark_symbol=None,
        )

    def fake_fit(
        candidate_config,
        train_period,
        training,
        database_manager,
        *,
        ridge_alpha=None,
        training_data_cache=None,
    ):
        fit_calls.append(
            (
                candidate_config.strategy_parameters["holding_count"],
                ridge_alpha,
            )
        )
        return candidate_config, FactorTrainingResult(
            factor_weights={"valuation_pb": 1.0},
            observation_count=20,
            signal_date_count=5,
            iterations=2,
            converged=True,
            label_horizon_days=20,
            ridge_alpha=ridge_alpha,
        )

    def fake_execute_backtest(
        candidate_config, database_manager, *, include_benchmark, execution_cache=None
    ):
        phase = (
            "validation"
            if candidate_config.start_date == split.validation.start_date
            else "test"
            if candidate_config.start_date == split.test.start_date
            else "train"
        )
        parameters = candidate_config.strategy_parameters
        run_calls.append((phase, parameters["holding_count"], parameters))
        preferred = parameters["holding_count"] == 20 and phase == "validation"
        score = 2.0 if preferred else 1.0
        daily_nav = pd.DataFrame([{"date": pd.Timestamp("2024-01-02"), "nav": 1.0}])
        daily_nav.attrs["score"] = score
        return BacktestRun(
            config=candidate_config,
            targets=pd.DataFrame(),
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    def fake_metrics(daily_nav):
        score = daily_nav.attrs["score"]
        return {
            "total_return": score,
            "annualized_return": score,
            "annualized_volatility": 1.0,
            "sharpe_ratio": score,
            "max_drawdown": -score,
            "trading_days": 1,
        }

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", fake_load_backtest_config
    )
    monkeypatch.setattr(evaluator_module, "_fit_candidate_config", fake_fit)
    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute_backtest)
    monkeypatch.setattr(evaluator_module, "calculate_performance_metrics", fake_metrics)

    result = evaluate_factor_experiments(config, object())

    assert len(fit_calls) == 8
    assert len(result.hyperparameter_rows) == 8
    assert all(
        row["factor_weights"] == '{"valuation_pb": 1.0}'
        for row in result.hyperparameter_rows
    )
    assert len(result.metric_rows) == 17
    assert len([call for call in run_calls if call[0] == "test"]) == 1
    selection = result.selection_rows[0]
    assert selection["selected_candidate"] == "candidate"
    assert selection["selected_holding_count"] == 20
    assert selection["selected_ridge_alpha"] == 0.1

    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "hyperparameter-report"
    )
    weight_diagnostics = pd.read_csv(output_dir / "factor_weight_diagnostics.csv")
    assert list(weight_diagnostics.columns) == list(
        evaluator_module.FACTOR_WEIGHT_DIAGNOSTIC_COLUMNS
    )
    assert len(weight_diagnostics) == 8
    assert set(weight_diagnostics["collapse_level"]) == {"single_factor"}
    assert int(weight_diagnostics["selected"].sum()) == 1
    assert weight_diagnostics["effective_factor_count"].tolist() == pytest.approx(
        [1.0] * 8
    )
    report = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "验证集 Top 10 组合" in report
    assert "全部训练组合的权重集中度" in report
    assert "valuation_pb=1.0000" in report
    assert "信号日数量" in report


def test_factor_weight_diagnostic_classifies_concentration_and_selection():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="weight-diagnostic",
        candidate_configs=(Path("candidate.toml"),),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
    )
    result = FactorExperimentEvaluationResult(
        config=config,
        metric_rows=(),
        selection_rows=(
            {
                "split_id": "fixed_split",
                "status": "failed",
                "error": "测试执行失败",
                "selected_candidate": "candidate",
                "selected_trial_id": "trial_single",
            },
        ),
        training_rows=(
            {
                "split_id": "fixed_split",
                "candidate_id": "candidate",
                "trial_id": "trial_equal",
                "status": "fitted",
                "factor_weights": '{"factor_a": 0.5, "factor_b": 0.5}',
            },
            {
                "split_id": "fixed_split",
                "candidate_id": "candidate",
                "trial_id": "trial_concentrated",
                "status": "fitted",
                "factor_weights": '{"factor_a": 0.8, "factor_b": 0.2}',
            },
            {
                "split_id": "fixed_split",
                "candidate_id": "candidate",
                "trial_id": "trial_single",
                "status": "fitted",
                "factor_weights": '{"factor_a": 1.0, "factor_b": 0.0}',
            },
            {
                "split_id": "fixed_split",
                "candidate_id": "candidate",
                "trial_id": "trial_failed",
                "status": "failed",
                "error": "训练失败",
                "factor_parameters": '{"factor_a": {}, "factor_b": {}}',
                "factor_weights": "",
            },
        ),
    )

    rows = evaluator_module._build_factor_weight_diagnostic_rows(result)
    by_trial = {row["trial_id"]: row for row in rows}

    assert by_trial["trial_equal"]["collapse_level"] == "diversified"
    assert by_trial["trial_equal"]["effective_factor_count"] == pytest.approx(2.0)
    assert by_trial["trial_equal"]["normalized_weight_entropy"] == pytest.approx(1.0)
    assert by_trial["trial_concentrated"]["collapse_level"] == "concentrated"
    assert by_trial["trial_single"]["collapse_level"] == "single_factor"
    assert by_trial["trial_single"]["selected"] is True
    assert by_trial["trial_failed"]["collapse_level"] == "not_available"
    assert by_trial["trial_failed"]["factor_count"] == 2
    assert by_trial["trial_failed"]["selected"] is False


def test_evaluation_report_summarizes_walk_forward_and_weight_stability(tmp_path):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    fixed_split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="walk-forward-report",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=fixed_split,
        walk_forward=WalkForwardSpec(
            start_date=date(2020, 1, 1),
            end_date=date(2024, 12, 31),
            train_years=1,
            validation_years=1,
            test_years=1,
            step_years=1,
        ),
        training=TrainingSpec(
            minimum_training_observations=2,
            minimum_training_dates=1,
        ),
    )
    selection_rows = []
    training_rows = []
    metric_rows = []
    for index, split in enumerate(config.get_splits(), start=1):
        trial_id = f"candidate__trial_{index:03d}"
        factor_weights = (
            '{"quality_roic": 0.9, "valuation_pb": 0.1}'
            if split.split_id == "fixed_split"
            else '{"quality_roic": 0.6, "valuation_pb": 0.4}'
        )
        training_rows.append(
            {
                "split_id": split.split_id,
                "candidate_id": "candidate",
                "trial_id": trial_id,
                "status": "fitted",
                "factor_weights": factor_weights,
                "observation_count": 100,
                "signal_date_count": 12,
                "signal_date_start": "2020-01-31",
                "signal_date_end": "2020-12-31",
            }
        )
        selection_rows.append(
            {
                "split_id": split.split_id,
                "selected_candidate": "candidate",
                "selected_trial_id": trial_id,
                "validation_score": 0.5,
                "test_total_return": 0.1,
                "test_annualized_return": 0.1,
                "test_annualized_volatility": 0.2,
                "test_sharpe_ratio": 0.5,
                "test_max_drawdown": -0.1,
                "test_trading_days": 240,
            }
        )
        for phase in ("train", "validation", "test"):
            metric_rows.append(
                {
                    "split_id": split.split_id,
                    "phase": phase,
                    "candidate_id": "candidate",
                    "trial_id": trial_id,
                    "total_return": 0.1,
                    "annualized_return": 0.1,
                    "annualized_volatility": 0.2,
                    "sharpe_ratio": 0.5,
                    "max_drawdown": -0.1,
                    "trading_days": 240,
                }
            )

    result = FactorExperimentEvaluationResult(
        config=config,
        metric_rows=tuple(metric_rows),
        selection_rows=tuple(selection_rows),
        training_rows=tuple(training_rows),
    )
    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "walk-forward-report"
    )
    report = output_dir.joinpath("summary.md").read_text(encoding="utf-8")

    assert "Walk-forward 权重稳定性" in report
    assert "Walk-forward 测试段汇总" in report
    assert "正收益窗口比例" in report
    assert "0.6000" in report
    assert "0.6975" not in report
    assert "completed" in report


def test_fit_candidate_config_caches_prepared_data_but_refits_weights(
    monkeypatch,
):
    candidate = BacktestConfig(
        start_date=date(2018, 1, 1),
        end_date=date(2026, 1, 1),
        strategy_name="factor-composite-experiment",
        strategy_parameters={
            "factor_weights": {"valuation_pb": 1.0},
            "holding_count": 20,
        },
        benchmark_symbol=None,
    )
    alternate = replace(
        candidate,
        strategy_parameters={
            **candidate.strategy_parameters,
            "holding_count": 50,
        },
    )
    train_period = EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31))
    training = TrainingSpec(
        minimum_training_observations=2,
        minimum_training_dates=1,
    )
    load_calls = []
    prepare_calls = []
    fit_calls = []

    class FakeDataAccess:
        def __init__(self, database_manager):
            assert database_manager == "database"

        def load_factor_data(self, *args, **kwargs):
            load_calls.append((args, kwargs))
            return pd.DataFrame(
                {
                    "date": [pd.Timestamp("2020-01-31")],
                    "symbol": ["000001"],
                    "open_hfq": [10.0],
                    "close_hfq": [10.1],
                }
            )

    def fake_prepare(*args, **kwargs):
        prepare_calls.append((args, kwargs))
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2020-01-31")],
                "symbol": ["000001"],
                "valuation_pb": [1.0],
                "label_exit_date": [pd.Timestamp("2020-02-28")],
                "forward_return_20d": [0.01],
            }
        )

    def fake_fit(*args, **kwargs):
        fit_calls.append((args, kwargs))
        return FactorTrainingResult(
            factor_weights={"valuation_pb": 1.0},
            observation_count=20,
            signal_date_count=5,
            iterations=2,
            converged=True,
            label_horizon_days=20,
            ridge_alpha=kwargs["ridge_alpha"],
        )

    monkeypatch.setattr(evaluator_module, "BacktestDataAccess", FakeDataAccess)
    monkeypatch.setattr(evaluator_module, "prepare_factor_training_data", fake_prepare)
    monkeypatch.setattr(evaluator_module, "fit_factor_weights", fake_fit)

    cache = {}
    trained_candidate, _ = evaluator_module._fit_candidate_config(
        candidate,
        train_period,
        training,
        "database",
        ridge_alpha=0.1,
        training_data_cache=cache,
    )
    evaluator_module._fit_candidate_config(
        alternate,
        train_period,
        training,
        "database",
        ridge_alpha=1.0,
        training_data_cache=cache,
    )

    assert len(load_calls) == 1
    assert len(prepare_calls) == 1
    assert len(fit_calls) == 2
    assert [call[1]["ridge_alpha"] for call in fit_calls] == [0.1, 1.0]
    assert "factor_versions" not in trained_candidate.strategy_parameters


def test_evaluation_config_rejects_overlapping_periods(tmp_path):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    config_path = tmp_path / "evaluation.toml"
    config_path.write_text(
        """
[experiment]
name = "invalid"
candidate_configs = ["candidate.toml"]

[split]
train_start_date = "2020-01-01"
train_end_date = "2021-01-01"
validation_start_date = "2021-01-01"
validation_end_date = "2022-01-01"
test_start_date = "2022-01-02"
test_end_date = "2023-01-01"
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="训练集与验证集日期不能重叠"):
        load_factor_experiment_evaluation_config(config_path)


def test_main_evaluation_entrypoint_writes_requested_output(monkeypatch, tmp_path):
    calls = {}

    class FakeConfig:
        name = "fake-evaluation"

    def fake_load(path):
        calls["config_path"] = path
        return FakeConfig()

    def fake_evaluate(config, database_manager):
        calls["config"] = config
        calls["database_manager"] = database_manager
        return "fake-result"

    def fake_write(result, output_path):
        calls["result"] = result
        calls["output_path"] = output_path
        return tmp_path / "written"

    monkeypatch.setattr(
        evaluator_module,
        "load_factor_experiment_evaluation_config",
        fake_load,
    )
    monkeypatch.setattr(
        evaluator_module,
        "evaluate_factor_experiments",
        fake_evaluate,
    )
    monkeypatch.setattr(
        evaluator_module,
        "write_factor_experiment_evaluation_report",
        fake_write,
    )

    output = main_module.evaluate_factor_experiments(
        "research.toml", str(tmp_path / "requested-output")
    )

    assert output == tmp_path / "written"
    assert calls["config_path"] == "research.toml"
    assert calls["result"] == "fake-result"
    assert calls["output_path"] == tmp_path / "requested-output"
