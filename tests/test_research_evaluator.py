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
    HyperparameterTrial,
    expand_hyperparameter_trials,
)
from backtest.research_evaluator import (
    BoundedTrainingDataCache,
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
from backtest.strategies.multi_factor_quality_value_momentum import (
    DEFAULT_FACTOR_WEIGHTS,
)


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
    assert config.training.max_training_cache_entries == 4
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


def test_loads_portfolio_weighting_production_research_config():
    config = load_factor_experiment_evaluation_config(
        "config/backtest/multi_factor_quality_value_momentum_portfolio_weighting_production.toml"
    )

    assert [path.stem for path in config.candidate_configs] == [
        "multi_factor_quality_value_momentum",
        "multi_factor_quality_value_momentum_rank_decay",
        "multi_factor_quality_value_momentum_inverse_volatility",
    ]
    assert config.hyperparameter_search is not None
    assert config.hyperparameter_search.max_combinations == 3
    assert config.hyperparameter_search.holding_counts == (50,)
    assert config.hyperparameter_search.ridge_alphas == (0.1,)
    candidate_configs = [
        (path.stem, load_backtest_config(path)) for path in config.candidate_configs
    ]
    assert (
        len(
            expand_hyperparameter_trials(
                candidate_configs, config.hyperparameter_search
            )
        )
        == 3
    )


@pytest.mark.parametrize(
    ("config_path", "expected_frequency", "expected_label_horizon"),
    [
        (
            "config/backtest/multi_factor_quality_value_momentum_monthly_expanded_grid_evaluation.toml",
            "monthly",
            20,
        ),
        (
            "config/backtest/multi_factor_quality_value_momentum_biweekly_expanded_grid_evaluation.toml",
            "biweekly",
            10,
        ),
    ],
)
def test_loads_frequency_specific_expanded_grid_research_configs(
    config_path, expected_frequency, expected_label_horizon
):
    config = load_factor_experiment_evaluation_config(config_path)

    assert config.training is not None
    assert config.training.label_horizon_days == expected_label_horizon
    assert config.validity is not None
    assert config.validity.maximum_trials_per_validation_signal_date == 0.5
    assert config.hyperparameter_search is not None
    assert config.hyperparameter_search.max_combinations == 24
    assert config.hyperparameter_search.holding_counts == (20, 30, 40, 50)
    assert config.hyperparameter_search.ridge_alphas == (0.1, 1.0)
    assert config.walk_forward is not None
    assert config.walk_forward.train_years == 6
    assert config.walk_forward.validation_years == 5
    assert config.walk_forward.test_years == 1
    assert config.walk_forward.step_years == 1
    splits = config.get_splits()
    assert len(splits) == 4
    walk_forward_splits = splits[1:]
    assert config.validity.require_non_overlapping_test_windows is True
    assert all(
        previous.test.end_date < current.test.start_date
        for previous, current in zip(
            walk_forward_splits, walk_forward_splits[1:], strict=False
        )
    )
    candidate_configs = [
        (path.stem, load_backtest_config(path)) for path in config.candidate_configs
    ]
    assert all(
        candidate_config.rebalance_frequency == expected_frequency
        for _, candidate_config in candidate_configs
    )
    assert (
        len(
            expand_hyperparameter_trials(
                candidate_configs, config.hyperparameter_search
            )
        )
        == 24
    )


def test_loads_formal_strategy_training_config():
    config = load_factor_experiment_evaluation_config(
        "config/backtest/multi_factor_quality_value_momentum_evaluation.toml"
    )

    assert (
        config.candidate_configs[0].name == "multi_factor_quality_value_momentum.toml"
    )
    assert config.training is not None
    assert config.training.max_training_cache_entries == 2
    assert config.training.minimum_training_dates == 48
    assert config.retrospective_method_development is True
    assert config.validity is not None
    assert config.validity.minimum_effective_factor_count == 2.0
    assert config.validity.maximum_factor_weight == 0.8
    assert config.validity.minimum_validation_selection_score == 0.0
    assert config.validity.maximum_training_failure_ratio == 0.0
    assert config.validity.maximum_trials_per_validation_signal_date == 0.25
    assert config.validity.minimum_completed_test_windows == 3
    assert config.validity.require_non_overlapping_test_windows is True
    assert config.hyperparameter_search is not None
    assert config.hyperparameter_search.ridge_alphas == (0.1, 1.0)
    assert config.hyperparameter_search.holding_counts == (20, 50)
    assert config.walk_forward is not None
    assert config.walk_forward.start_date == date(2011, 1, 1)
    assert config.walk_forward.train_years == 5
    assert config.walk_forward.step_years == 2
    assert len(config.get_splits()) == 5
    candidate_configs = [
        (path.stem, load_backtest_config(path)) for path in config.candidate_configs
    ]
    assert (
        len(
            expand_hyperparameter_trials(
                candidate_configs, config.hyperparameter_search
            )
        )
        == 4
    )


def test_bounded_training_data_cache_evicts_least_recently_used_entry():
    cache = BoundedTrainingDataCache(max_entries=2)
    first = pd.DataFrame({"value": [1]})
    second = pd.DataFrame({"value": [2]})
    third = pd.DataFrame({"value": [3]})

    cache["first"] = first
    cache["second"] = second
    assert cache["first"] is first
    cache["third"] = third

    assert "first" in cache
    assert "second" not in cache
    assert cache["third"] is third
    assert cache.stats == {"entries": 2, "max_entries": 2, "evictions": 1}


def test_bounded_training_data_cache_can_evict_before_replacement_is_loaded():
    cache = BoundedTrainingDataCache(max_entries=1)
    cache["first"] = pd.DataFrame({"value": [1]})

    cache.reserve_entry("second")

    assert len(cache) == 0
    assert cache.stats == {"entries": 0, "max_entries": 1, "evictions": 1}


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


def test_research_protocol_rejects_overlapping_walk_forward_test_windows():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2010, 1, 1), date(2014, 12, 31)),
        validation=EvaluationPeriod(date(2015, 1, 1), date(2016, 12, 31)),
        test=EvaluationPeriod(date(2017, 1, 1), date(2018, 12, 31)),
    )

    with pytest.raises(
        evaluator_module.ResearchValidityError,
        match="walk_forward_test_overlap",
    ):
        FactorExperimentEvaluationConfig(
            name="overlapping-tests",
            candidate_configs=(Path("candidate.toml"),),
            selection_metric="sharpe_ratio",
            selection_direction="max",
            fixed_split=split,
            walk_forward=WalkForwardSpec(
                start_date=date(2010, 1, 1),
                end_date=date(2022, 12, 31),
                train_years=5,
                validation_years=2,
                test_years=2,
                step_years=1,
            ),
            validity=ResearchValiditySpec(require_non_overlapping_test_windows=True),
        )


def test_training_model_gate_rejects_concentrated_weights_before_backtest(
    monkeypatch, tmp_path
):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="model-gate",
        candidate_configs=(candidate,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=1,
            minimum_training_dates=1,
        ),
        validity=ResearchValiditySpec(
            minimum_training_signal_dates=1,
            minimum_validation_signal_dates=1,
            minimum_test_signal_dates=1,
            minimum_validation_observations=1,
            minimum_test_observations=1,
            minimum_effective_factor_count=2,
            maximum_factor_weight=0.8,
        ),
    )

    monkeypatch.setattr(
        evaluator_module,
        "load_backtest_config",
        lambda path: BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name="factor-composite-experiment",
            strategy_parameters={
                "factor_weights": {"valuation_pb": 0.5, "quality_roic": 0.5}
            },
            benchmark_symbol=None,
        ),
    )
    monkeypatch.setattr(
        evaluator_module,
        "_fit_candidate_config",
        lambda *args, **kwargs: (
            args[0],
            FactorTrainingResult(
                factor_weights={"valuation_pb": 0.9, "quality_roic": 0.1},
                observation_count=20,
                signal_date_count=5,
                iterations=2,
                converged=True,
                label_horizon_days=20,
                ridge_alpha=0.1,
            ),
        ),
    )
    monkeypatch.setattr(
        evaluator_module,
        "execute_backtest",
        lambda *args, **kwargs: pytest.fail("模型门禁失败后不应运行回测"),
    )

    result = evaluate_factor_experiments(config, object())

    training_gate = result.validity_rows[0]
    assert training_gate["status"] == "failed"
    assert "training_model" in training_gate["error"]
    assert result.training_rows[0]["status"] == "rejected"
    assert result.selection_rows[0]["status"] == "rejected"


def test_negative_validation_score_is_rejected_without_reading_test(
    monkeypatch, tmp_path
):
    candidate = tmp_path / "candidate.toml"
    candidate.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="validation-rejection",
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
            minimum_validation_selection_score=0.0,
        ),
    )
    calls = []

    monkeypatch.setattr(
        evaluator_module,
        "load_backtest_config",
        lambda path: BacktestConfig(
            start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1),
            strategy_name=path.stem,
            benchmark_symbol=None,
        ),
    )

    def fake_execute(candidate_config, *args, **kwargs):
        del args, kwargs
        phase = (
            "validation"
            if candidate_config.start_date == split.validation.start_date
            else "test"
            if candidate_config.start_date == split.test.start_date
            else "train"
        )
        calls.append(phase)
        if phase == "test":
            pytest.fail("负验证分数被拒绝后不应读取测试段")
        signal_date = pd.Timestamp(candidate_config.start_date) + pd.Timedelta(days=1)
        daily_nav = pd.DataFrame(
            {
                "date": [signal_date, signal_date + pd.Timedelta(days=1)],
                "nav": [1.0, 0.99],
            }
        )
        daily_nav.attrs["score"] = -0.5 if phase == "validation" else 0.5
        return BacktestRun(
            config=candidate_config,
            targets=pd.DataFrame({"date": [signal_date], "symbol": ["000001"]}),
            result=BacktestResult(daily_nav=daily_nav, trades=pd.DataFrame()),
        )

    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute)
    monkeypatch.setattr(
        evaluator_module,
        "calculate_performance_metrics",
        lambda daily_nav: {
            "total_return": daily_nav.attrs["score"],
            "annualized_return": daily_nav.attrs["score"],
            "annualized_volatility": 1.0,
            "sharpe_ratio": daily_nav.attrs["score"],
            "max_drawdown": -0.1,
            "trading_days": 2,
        },
    )

    result = evaluate_factor_experiments(config, object())

    assert calls == ["train", "validation"]
    assert result.selection_rows[0]["status"] == "rejected"
    assert result.selection_rows[0]["selected_trial_id"] == ""
    assert "预设模型或验证门禁拒绝" in result.selection_rows[0]["error"]
    selection_gate = next(
        row for row in result.validity_rows if row["phase"] == "validation_selection"
    )
    assert selection_gate["status"] == "rejected"
    assert not any(row["phase"] == "test" for row in result.metric_rows)
    diagnostic = result.selection_diagnostic_rows[0]
    assert diagnostic["candidate_count"] == 1
    assert diagnostic["fitted_trial_count"] == 1
    assert diagnostic["validation_signal_date_count"] == 1
    assert diagnostic["trials_per_validation_signal_date"] == pytest.approx(1.0)


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


def test_split_protocol_checks_training_failures_and_selection_burden():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    validity = ResearchValiditySpec(
        maximum_training_failure_ratio=0.0,
        maximum_trials_per_validation_signal_date=0.25,
    )
    protocol_rows = []

    error = evaluator_module._append_split_protocol_checks(
        protocol_rows,
        split,
        trial_count=4,
        training_fit_failure_count=1,
        validation_coverage={
            f"trial_{index}": {"signal_date_count": 20} for index in range(4)
        },
        validity=validity,
    )

    assert "training_failure_ratio" in error
    assert protocol_rows[0]["status"] == "failed"
    assert protocol_rows[0]["actual_value"] == pytest.approx(0.25)
    assert protocol_rows[1]["status"] == "passed"
    assert protocol_rows[1]["actual_value"] == pytest.approx(0.2)


def test_retrospective_run_never_claims_confirmatory_strategy_evidence():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="retrospective-evidence",
        candidate_configs=(Path("candidate.toml"),),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        retrospective_method_development=True,
    )

    status = evaluator_module._determine_strategy_evidence_status(
        config,
        "protocol_passed",
        [{"status": "completed"}] * 10,
    )

    assert status == "retrospective_descriptive_only"


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

    assert result.selection_rows[0]["status"] == "rejected"
    assert result.training_rows[0]["status"] == "rejected"
    assert result.training_rows[0]["signal_date_count"] == 1
    assert result.validity_rows[0]["phase"] == "training"
    assert result.validity_rows[0]["status"] == "failed"
    assert "minimum_signal_dates=2" in result.validity_rows[0]["error"]


def test_research_validity_report_contains_phase_coverage(tmp_path, monkeypatch):
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

    monkeypatch.setattr(evaluator_module, "_get_process_peak_rss_bytes", lambda: 1024)
    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "validity-report-output"
    )

    assert output_dir.joinpath("research_validity.csv").exists()
    assert output_dir.joinpath("research_protocol.csv").exists()
    parameters = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert parameters["audit"]["resource"]["peak_rss_bytes"] == 1024
    assert parameters["audit"]["resource"]["peak_rss_exceeded"] is False
    assert parameters["protocol_status"] == "protocol_not_evaluated"
    assert parameters["strategy_evidence_status"] == ("strategy_evidence_not_evaluated")
    report = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "研究有效性门禁" in report
    assert "资源审计" in report
    assert "research_validity.csv" in report


def test_training_model_report_persists_rebalance_schedule_metadata(
    tmp_path, monkeypatch
):
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    weekly_config = BacktestConfig(
        start_date=date(2018, 1, 1),
        end_date=date(2026, 1, 1),
        strategy_name="factor-composite-experiment",
        rebalance_frequency="weekly",
        benchmark_symbol=None,
    )
    every_n_config = replace(
        weekly_config,
        rebalance_frequency="every_n_trading_days",
        rebalance_interval_trading_days=5,
    )
    weekly_trial = HyperparameterTrial(
        trial_id="weekly_trial",
        candidate_id="weekly",
        config=weekly_config,
        parameters={},
    )
    every_n_trial = HyperparameterTrial(
        trial_id="every_n_trial",
        candidate_id="every_n",
        config=every_n_config,
        parameters={},
    )
    weekly_training = FactorTrainingResult(
        factor_weights={"valuation_pb": 1.0},
        observation_count=100,
        signal_date_count=50,
        iterations=2,
        converged=True,
        label_horizon_days=20,
        ridge_alpha=0.1,
        prior_factor_weights={"valuation_pb": 1.0},
        rebalance_frequency="weekly",
    )
    every_n_training = replace(
        weekly_training,
        rebalance_frequency="every_n_trading_days",
        rebalance_interval_trading_days=5,
        rebalance_anchor_date="2020-01-01",
    )
    fitted_row = evaluator_module._build_training_row(
        split, weekly_trial, weekly_training
    )
    rejected_row = evaluator_module._build_failed_training_row(
        split,
        every_n_trial,
        evaluator_module.ResearchValidityError("模型门禁拒绝"),
        training_result=every_n_training,
    )
    rejected_row["status"] = "rejected"
    failed_row = evaluator_module._build_failed_training_row(
        split,
        every_n_trial,
        ValueError("训练失败"),
    )
    config = FactorExperimentEvaluationConfig(
        name="schedule-metadata",
        candidate_configs=(tmp_path / "candidate.toml",),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=1,
            minimum_training_dates=1,
        ),
    )
    result = FactorExperimentEvaluationResult(
        config=config,
        metric_rows=(),
        selection_rows=(),
        training_rows=(fitted_row, rejected_row, failed_row),
    )
    monkeypatch.setattr(evaluator_module, "_build_reproducibility_audit", lambda *_: {})
    monkeypatch.setattr(evaluator_module, "_write_evaluation_summary", lambda *_: None)

    output_dir = write_factor_experiment_evaluation_report(
        result, tmp_path / "schedule-metadata-output"
    )
    models = pd.read_csv(output_dir / "training_models.csv")

    assert models["rebalance_frequency"].tolist() == [
        "weekly",
        "every_n_trading_days",
        "every_n_trading_days",
    ]
    assert pd.isna(models.loc[0, "rebalance_interval_trading_days"])
    assert models.loc[1:, "rebalance_interval_trading_days"].tolist() == [5.0, 5.0]
    assert pd.isna(models.loc[0, "rebalance_anchor_date"])
    assert models.loc[1:, "rebalance_anchor_date"].tolist() == [
        "2020-01-01",
        "2020-01-01",
    ]


def test_report_uses_test_refit_factor_weights_for_selected_model():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="refit-weight-report",
        candidate_configs=(Path("candidate.toml"),),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=1,
            minimum_training_dates=1,
            refit_selected_on_train_validation=True,
        ),
    )
    result = FactorExperimentEvaluationResult(
        config=config,
        metric_rows=(),
        training_rows=(
            {
                "split_id": "fixed_split",
                "trial_id": "trial_001",
                "status": "fitted",
                "factor_weights": '{"initial_factor": 1.0}',
            },
        ),
        selection_rows=(
            {
                "split_id": "fixed_split",
                "status": "completed",
                "selected_candidate": "candidate",
                "selected_trial_id": "trial_001",
                "selected_factor_parameters": "{}",
                "selected_factor_weights": '{"refit_factor": 1.0}',
                "selected_holding_count": 20,
                "selected_winsorize_lower": 0.05,
                "selected_winsorize_upper": 0.95,
                "selected_ridge_alpha": 0.1,
                "test_refit_performed": True,
            },
        ),
    )

    report = evaluator_module._build_evaluation_summary(result, audit={})

    assert "refit_factor=1.0000" in report
    assert "initial_factor=1.0000" not in report


def test_report_separates_successful_training_from_validation_rejection():
    split = EvaluationSplit(
        split_id="fixed_split",
        train=EvaluationPeriod(date(2020, 1, 1), date(2020, 12, 31)),
        validation=EvaluationPeriod(date(2021, 1, 1), date(2021, 12, 31)),
        test=EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="training-versus-validation-status",
        candidate_configs=(Path("candidate.toml"),),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=1,
            minimum_training_dates=1,
        ),
        hyperparameter_search=HyperparameterSearchSpec(
            holding_counts=(20, 50),
            winsorize_ranges=((0.05, 0.95),),
            ridge_alphas=(0.1, 1.0),
        ),
    )
    training_rows = tuple(
        {
            "split_id": "fixed_split",
            "candidate_id": "candidate",
            "trial_id": f"trial_{index}",
            "status": "fitted",
            "error": "",
            "factor_weights": '{"valuation_pb": 1.0}',
            "factor_parameters": '{"valuation_pb": {}}',
            "signal_date_count": 48,
            "observation_count": 1000,
        }
        for index in range(4)
    )
    hyperparameter_rows = tuple(
        {
            "split_id": "fixed_split",
            "candidate_id": "candidate",
            "trial_id": f"trial_{index}",
            "status": "rejected",
            "error": "验证分数未过门禁",
            "holding_count": 20,
            "winsorize_lower": 0.05,
            "winsorize_upper": 0.95,
            "ridge_alpha": 0.1,
            "factor_parameters": '{"valuation_pb": {}}',
            "factor_weights": '{"valuation_pb": 1.0}',
            "train_sharpe_ratio": 0.5,
            "validation_sharpe_ratio": -0.1,
        }
        for index in range(4)
    )
    result = FactorExperimentEvaluationResult(
        config=config,
        metric_rows=(),
        selection_rows=(
            {
                "split_id": "fixed_split",
                "status": "rejected",
                "selected_candidate": "",
                "selected_trial_id": "",
            },
        ),
        training_rows=training_rows,
        hyperparameter_rows=hyperparameter_rows,
        protocol_status="protocol_passed",
    )

    report = evaluator_module._build_evaluation_summary(result, {})

    assert "| 成功拟合数量 | 4 |" in report
    assert "| 训练失败数量 | 0 |" in report
    assert "| 验证门禁拒绝数量 | 4 |" in report
    assert "| fixed_split | 4 | 4 | 0 | 0 |" in report
    assert report.count("验证拒绝") >= 4
    assert "没有成功拟合的组合" not in report


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


def test_fit_candidate_config_supports_formal_strategy_and_preserves_zero_weights(
    monkeypatch,
):
    candidate = BacktestConfig(
        start_date=date(2018, 1, 1),
        end_date=date(2026, 1, 1),
        strategy_name="multi-factor-quality-value-momentum",
        strategy_parameters={"holding_count": 20},
        benchmark_symbol=None,
    )
    train_period = EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31))
    training = TrainingSpec(
        minimum_training_observations=2,
        minimum_training_dates=1,
    )
    factor_names = tuple(DEFAULT_FACTOR_WEIGHTS)
    fitted_weights = {name: 0.0 for name in factor_names}
    fitted_weights["price_momentum_120d"] = 1.0

    class FakeDataAccess:
        def __init__(self, database_manager):
            assert database_manager == "database"

        def load_factor_data(self, *args, **kwargs):
            return pd.DataFrame()

    def fake_prepare(*args, **kwargs):
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2020-01-31")],
                "symbol": ["000001"],
                **{name: [1.0] for name in factor_names},
                "label_exit_date": [pd.Timestamp("2020-02-28")],
                "forward_return_20d": [0.01],
            }
        )

    def fake_fit(*args, **kwargs):
        return FactorTrainingResult(
            factor_weights=fitted_weights,
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

    trained_candidate, _ = evaluator_module._fit_candidate_config(
        candidate,
        train_period,
        training,
        "database",
        ridge_alpha=0.1,
    )

    trained_parameters = trained_candidate.strategy_parameters
    assert set(trained_parameters["factor_weights"]) == set(factor_names)
    assert trained_parameters["factor_weights"]["valuation_pe_ttm"] == 0.0
    resolved = evaluator_module.get_backtest_strategy(
        "multi-factor-quality-value-momentum"
    ).validate_parameters(trained_parameters)
    assert resolved["factor_weights"]["valuation_pe_ttm"] == 0.0


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
