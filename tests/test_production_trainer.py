"""生产因子模型参数锁定、标签截断、最终重训和初始化目标测试。"""

from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import backtest.research_evaluator as evaluator_module
from backtest.config import BacktestConfig
from backtest.engine import BacktestResult
from backtest.factor_trainer import FactorTrainingResult
from backtest.hyperparameter_search import (
    HyperparameterSearchSpec,
    HyperparameterTrial,
)
from backtest.production_trainer import (
    FactorProductionTrainingConfig,
    FactorProductionTrainingResult,
    ProductionTrainingSpec,
    build_initial_deployment_targets,
    load_factor_production_training_config,
    resolve_production_training_dates,
    select_locked_production_trial,
    train_factor_production_model,
    write_factor_production_training_report,
)
from backtest.research_evaluator import (
    EvaluationPeriod,
    EvaluationSplit,
    FactorExperimentEvaluationConfig,
    FactorExperimentEvaluationResult,
    ResearchValiditySpec,
    TrainingSpec,
    WalkForwardSpec,
    evaluate_factor_experiments,
)
from backtest.runner import BacktestRun

FACTOR_NAMES = (
    "price_momentum_120d",
    "price_trend_gap_120d",
    "price_volatility_60d",
    "valuation_pe_ttm",
    "valuation_pb",
    "quality_roe_weighted",
    "quality_operating_cashflow_ratio",
)


def _candidate_config() -> BacktestConfig:
    return BacktestConfig(
        start_date=date(2012, 1, 1),
        end_date=date(2026, 8, 21),
        strategy_name="multi-factor-quality-value-momentum",
        strategy_parameters={
            "holding_count": 2,
            "min_listing_days": 250,
            "winsorize_lower": 0.05,
            "winsorize_upper": 0.95,
            "factor_weights": {name: 1 / len(FACTOR_NAMES) for name in FACTOR_NAMES},
        },
        benchmark_symbol=None,
    )


def _research_config() -> FactorExperimentEvaluationConfig:
    return FactorExperimentEvaluationConfig(
        name="production-test",
        candidate_configs=(Path("candidate.toml"),),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=EvaluationSplit(
            "fixed_split",
            EvaluationPeriod(date(2012, 1, 1), date(2013, 12, 31)),
            EvaluationPeriod(date(2014, 1, 1), date(2014, 12, 31)),
            EvaluationPeriod(date(2015, 1, 1), date(2015, 12, 31)),
        ),
        walk_forward=WalkForwardSpec(
            start_date=date(2012, 1, 1),
            end_date=date(2017, 12, 31),
            train_years=2,
            validation_years=1,
            test_years=1,
            step_years=1,
        ),
        training=TrainingSpec(
            minimum_training_dates=2,
            refit_selected_on_train_validation=True,
        ),
        hyperparameter_search=HyperparameterSearchSpec(
            max_combinations=2,
            holding_counts=(20, 50),
        ),
        validity=ResearchValiditySpec(
            minimum_training_signal_dates=2,
            minimum_completed_test_windows=1,
        ),
    )


def _trials() -> tuple[HyperparameterTrial, HyperparameterTrial]:
    config = _candidate_config()
    return (
        HyperparameterTrial(
            "candidate__trial_001",
            "candidate",
            config,
            {
                "holding_count": 20,
                "winsorize_lower": 0.05,
                "winsorize_upper": 0.95,
                "ridge_alpha": 0.1,
                "portfolio_weighting": "equal",
                "factor_parameters": {},
            },
        ),
        HyperparameterTrial(
            "candidate__trial_002",
            "candidate",
            config,
            {
                "holding_count": 50,
                "winsorize_lower": 0.05,
                "winsorize_upper": 0.95,
                "ridge_alpha": 0.1,
                "portfolio_weighting": "equal",
                "factor_parameters": {},
            },
        ),
    )


def _research_result(scores_by_trial) -> FactorExperimentEvaluationResult:
    rows = []
    for trial_id, scores in scores_by_trial.items():
        for index, score in enumerate(scores, start=1):
            rows.append(
                {
                    "split_id": f"walk_forward_{index:02d}",
                    "candidate_id": "candidate",
                    "trial_id": trial_id,
                    "status": "fitted",
                    "validation_sharpe_ratio": score,
                }
            )
    return FactorExperimentEvaluationResult(
        config=_research_config(),
        metric_rows=(
            {
                "split_id": "walk_forward_01",
                "phase": "test",
                "sharpe_ratio": 999.0,
            },
        ),
        selection_rows=(),
        hyperparameter_rows=tuple(rows),
        protocol_status="protocol_passed",
    )


def _training_result(signal_date_end: str) -> FactorTrainingResult:
    return FactorTrainingResult(
        factor_weights={name: 1 / len(FACTOR_NAMES) for name in FACTOR_NAMES},
        observation_count=1000,
        signal_date_count=60,
        iterations=10,
        converged=True,
        label_horizon_days=20,
        ridge_alpha=0.1,
        signal_date_start="2020-08-01",
        signal_date_end=signal_date_end,
    )


def test_select_locked_trial_uses_validation_only_and_is_deterministic():
    result = _research_result(
        {
            "candidate__trial_001": [0.1, 0.2, 0.3],
            "candidate__trial_002": [0.15, 0.15, 0.15],
        }
    )

    selected, rows = select_locked_production_trial(result, _trials(), 3)

    assert selected.trial_id == "candidate__trial_001"
    assert rows[0]["validation_score_median"] == 0.2
    assert rows[0]["selected"] is True
    # 测试指标即使极端，也不在生产参数选择输入中。
    changed_test_result = replace(
        result,
        metric_rows=({"phase": "test", "sharpe_ratio": -999.0},),
    )
    selected_after_test_change, _ = select_locked_production_trial(
        changed_test_result, _trials(), 3
    )
    assert selected_after_test_change.trial_id == selected.trial_id


def test_loads_biweekly_inverse_volatility_production_config():
    config = load_factor_production_training_config(
        "config/backtest/multi_factor_quality_value_momentum_biweekly_inverse_volatility_production.toml"
    )

    assert config.research.training is not None
    assert config.research.training.label_horizon_days == 10
    assert config.research.hyperparameter_search is not None
    assert config.research.hyperparameter_search.max_combinations == 8
    assert config.research.hyperparameter_search.holding_counts == (20, 30, 40, 50)
    assert config.research.hyperparameter_search.ridge_alphas == (0.1, 1.0)
    assert config.research.candidate_configs[0].name == (
        "multi_factor_quality_value_momentum_inverse_volatility_biweekly.toml"
    )
    assert config.production.minimum_validation_windows == 3


def test_select_locked_trial_prioritizes_validation_window_coverage():
    result = _research_result(
        {
            "candidate__trial_001": [0.1, 0.1, 0.1],
            "candidate__trial_002": [0.9, 0.9],
        }
    )

    selected, _ = select_locked_production_trial(result, _trials(), 2)

    assert selected.trial_id == "candidate__trial_001"


def test_resolve_production_training_dates_excludes_incomplete_labels():
    trading_dates = pd.bdate_range("2020-01-01", periods=100)
    as_of_date = trading_dates[-2].date()

    latest, label_complete, training_start, next_execution = (
        resolve_production_training_dates(
            trading_dates,
            as_of_date=as_of_date,
            label_horizon_days=20,
            training_years=6,
        )
    )

    assert latest == trading_dates[-2].date()
    assert label_complete == trading_dates[-22].date()
    assert training_start == label_complete.replace(year=label_complete.year - 6)
    assert next_execution == trading_dates[-1].date()


def test_train_production_model_refits_through_latest_label_complete_window(
    monkeypatch,
):
    import backtest.production_trainer as production_module

    research = _research_result(
        {
            "candidate__trial_001": [0.1, 0.2, 0.3],
            "candidate__trial_002": [0.1, 0.1, 0.1],
        }
    )
    config = FactorProductionTrainingConfig(
        research=research.config,
        production=ProductionTrainingSpec(
            training_years=6,
            minimum_validation_windows=3,
        ),
    )
    trials = _trials()
    trading_dates = pd.bdate_range("2020-01-01", periods=100)
    as_of_date = trading_dates[-1].date()
    label_complete_date = trading_dates[-21].date()
    fitted_periods = []

    monkeypatch.setattr(
        production_module,
        "build_research_data_snapshot",
        lambda manager: {"snapshot_id": "data-test"},
    )
    monkeypatch.setattr(
        production_module,
        "verify_research_data_snapshot_unchanged",
        lambda start, end: {
            **start,
            "end_snapshot_id": end["snapshot_id"],
            "verified_unchanged_during_run": True,
        },
    )
    monkeypatch.setattr(
        production_module, "evaluate_factor_experiments", lambda *args: research
    )
    monkeypatch.setattr(
        production_module, "build_production_trials", lambda config: trials
    )
    monkeypatch.setattr(
        production_module.BacktestDataAccess,
        "load_trading_dates",
        lambda self: trading_dates,
    )

    def fake_fit(candidate, period, training, manager, ridge_alpha):
        fitted_periods.append((period, ridge_alpha))
        return candidate, _training_result(label_complete_date.isoformat())

    monkeypatch.setattr(production_module, "fit_factor_candidate_config", fake_fit)
    validated = []
    monkeypatch.setattr(
        production_module,
        "validate_factor_training_result",
        lambda *args: validated.append(args),
    )
    expected_targets = pd.DataFrame(
        {
            "date": [pd.Timestamp(as_of_date)],
            "symbol": ["600519"],
            "score": [1.0],
            "rank": [1],
            "target_weight": [1.0],
        }
    )
    monkeypatch.setattr(
        production_module,
        "build_initial_deployment_targets",
        lambda *args, **kwargs: expected_targets,
    )

    result = train_factor_production_model(config, object(), as_of_date=as_of_date)

    period, ridge_alpha = fitted_periods[0]
    assert period.end_date == as_of_date
    assert period.start_date == label_complete_date.replace(
        year=label_complete_date.year - 6
    )
    assert ridge_alpha == 0.1
    assert result.label_complete_date == label_complete_date
    assert result.targets.equals(expected_targets)
    assert len(validated) == 1


def test_research_evaluator_refits_selected_on_train_and_validation_before_test(
    monkeypatch, tmp_path
):
    candidate_path = tmp_path / "candidate.toml"
    candidate_path.write_text("", encoding="utf-8")
    split = EvaluationSplit(
        "fixed_split",
        EvaluationPeriod(date(2020, 1, 1), date(2021, 12, 31)),
        EvaluationPeriod(date(2022, 1, 1), date(2022, 12, 31)),
        EvaluationPeriod(date(2023, 1, 1), date(2023, 12, 31)),
    )
    config = FactorExperimentEvaluationConfig(
        name="test-refit",
        candidate_configs=(candidate_path,),
        selection_metric="sharpe_ratio",
        selection_direction="max",
        fixed_split=split,
        training=TrainingSpec(
            minimum_training_observations=1,
            minimum_training_dates=1,
            refit_selected_on_train_validation=True,
        ),
    )
    candidate_config = BacktestConfig(
        start_date=date(2018, 1, 1),
        end_date=date(2025, 1, 1),
        strategy_name="factor-composite-experiment",
        strategy_parameters={"factor_weights": {"valuation_pb": 1.0}},
        benchmark_symbol=None,
    )
    fit_periods = []
    run_calls = []

    monkeypatch.setattr(
        evaluator_module, "load_backtest_config", lambda path: candidate_config
    )

    def fake_fit(candidate, period, training, manager):
        fit_periods.append(period)
        fitted_weight = 0.8 if len(fit_periods) == 1 else 0.6
        fitted = replace(
            candidate,
            strategy_parameters={
                "factor_weights": {
                    "valuation_pb": fitted_weight,
                    "quality_roic": 1 - fitted_weight,
                }
            },
        )
        return fitted, FactorTrainingResult(
            factor_weights=fitted.strategy_parameters["factor_weights"],
            observation_count=100,
            signal_date_count=24,
            iterations=2,
            converged=True,
            label_horizon_days=20,
            ridge_alpha=0.1,
            signal_date_start=period.start_date.isoformat(),
            signal_date_end=period.end_date.isoformat(),
        )

    monkeypatch.setattr(evaluator_module, "_fit_candidate_config", fake_fit)

    def fake_execute(config, manager, *, include_benchmark, execution_cache=None):
        run_calls.append((config.start_date, config.strategy_parameters))
        return BacktestRun(
            config=config,
            targets=pd.DataFrame(),
            result=BacktestResult(
                daily_nav=pd.DataFrame(
                    {"date": [pd.Timestamp(config.start_date)], "nav": [1.0]}
                ),
                trades=pd.DataFrame(),
            ),
        )

    monkeypatch.setattr(evaluator_module, "execute_backtest", fake_execute)
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

    assert fit_periods == [
        split.train,
        EvaluationPeriod(split.train.start_date, split.validation.end_date),
    ]
    assert [start for start, _parameters in run_calls] == [
        split.train.start_date,
        split.validation.start_date,
        split.test.start_date,
    ]
    assert run_calls[-1][1]["factor_weights"] == {
        "valuation_pb": 0.6,
        "quality_roic": 0.4,
    }
    assert result.selection_rows[0]["test_refit_performed"] is True
    assert result.selection_rows[0]["test_refit_train_end_date"] == "2022-12-31"


def test_build_initial_deployment_targets_uses_latest_close(monkeypatch):
    import backtest.production_trainer as production_module

    config = _candidate_config()
    dates = pd.bdate_range(end="2026-08-21", periods=251)
    signal_data = pd.DataFrame(
        [
            {"date": trading_date, "symbol": symbol}
            for symbol in ("600519", "000001")
            for trading_date in dates
        ]
    )
    monkeypatch.setattr(
        production_module.BacktestDataAccess,
        "load_factor_data",
        lambda self, *args, **kwargs: signal_data,
    )

    class FakeFactorEngine:
        def calculate_factors_on_dates(
            self, data, factor_names, factor_parameters, signal_dates, **kwargs
        ):
            rows = []
            for index, symbol in enumerate(("600519", "000001"), start=1):
                row = {"date": signal_dates[0], "symbol": symbol}
                row.update({name: float(index) for name in factor_names})
                rows.append(row)
            return pd.DataFrame(rows)

    monkeypatch.setattr(production_module, "FactorEngine", FakeFactorEngine)

    targets = build_initial_deployment_targets(
        config,
        object(),
        signal_date=date(2026, 8, 21),
        next_execution_date=date(2026, 8, 24),
    )

    assert len(targets) == 2
    assert set(targets["target_kind"]) == {"initial_deployment"}
    assert set(targets["earliest_execution_date"]) == {"2026-08-24"}
    assert targets["target_weight"].sum() == 1.0


def test_build_initial_deployment_targets_rejects_insufficient_candidates(monkeypatch):
    import backtest.production_trainer as production_module

    config = _candidate_config()
    dates = pd.bdate_range(end="2026-08-21", periods=251)
    signal_data = pd.DataFrame(
        [{"date": trading_date, "symbol": "600519"} for trading_date in dates]
    )
    monkeypatch.setattr(
        production_module.BacktestDataAccess,
        "load_factor_data",
        lambda self, *args, **kwargs: signal_data,
    )

    class FakeFactorEngine:
        def calculate_factors_on_dates(
            self, data, factor_names, factor_parameters, signal_dates, **kwargs
        ):
            return pd.DataFrame(
                [
                    {
                        "date": signal_dates[0],
                        "symbol": "600519",
                        **dict.fromkeys(factor_names, 1.0),
                    }
                ]
            )

    monkeypatch.setattr(production_module, "FactorEngine", FakeFactorEngine)

    with pytest.raises(ValueError, match="有效生产目标不足"):
        build_initial_deployment_targets(
            config,
            object(),
            signal_date=date(2026, 8, 21),
            next_execution_date=date(2026, 8, 24),
        )


def test_project_production_config_loads():
    config = load_factor_production_training_config(
        "config/backtest/multi_factor_quality_value_momentum_production.toml"
    )

    assert config.production.training_years == 6


def test_portfolio_weighting_production_config_loads():
    config = load_factor_production_training_config(
        "config/backtest/multi_factor_quality_value_momentum_portfolio_weighting_production.toml"
    )

    assert len(config.research.candidate_configs) == 3
    assert config.research.hyperparameter_search is not None
    assert config.research.hyperparameter_search.max_combinations == 3
    assert config.production.minimum_validation_windows == 4
    assert config.research.fixed_split.train.start_date == date(2012, 1, 1)
    assert config.research.fixed_split.test.start_date == date(2023, 1, 1)


def test_write_production_report_creates_standard_artifacts(tmp_path, monkeypatch):
    import backtest.production_trainer as production_module

    research = _research_result(
        {
            "candidate__trial_001": [0.1, 0.2, 0.3],
            "candidate__trial_002": [0.1, 0.1, 0.1],
        }
    )
    config = FactorProductionTrainingConfig(
        research=research.config,
        production=ProductionTrainingSpec(6, 3),
    )
    trial = _trials()[0]
    trained_config = trial.config.with_resolved_strategy(
        "1",
        trial.config.strategy_parameters,
    )
    result = FactorProductionTrainingResult(
        config=config,
        as_of_date=date(2026, 8, 23),
        latest_market_date=date(2026, 8, 21),
        label_complete_date=date(2026, 7, 24),
        production_train_start_date=date(2020, 7, 24),
        next_execution_date=date(2026, 8, 24),
        locked_trial=trial,
        validation_selection_rows=({"trial_id": trial.trial_id, "selected": True},),
        trained_config=trained_config,
        training_result=_training_result("2026-07-24"),
        targets=pd.DataFrame(
            {
                "date": ["2026-08-21"],
                "symbol": ["600519"],
                "score": [1.0],
                "rank": [1],
                "target_weight": [1.0],
                "target_kind": ["initial_deployment"],
                "earliest_execution_date": ["2026-08-24"],
            }
        ),
        research_result=research,
        data_snapshot={
            "snapshot_id": "data-test",
            "verified_unchanged_during_run": True,
        },
    )

    def fake_write_research_report(research_result, output_path):
        Path(output_path).mkdir()
        return Path(output_path)

    monkeypatch.setattr(
        production_module,
        "write_factor_experiment_evaluation_report",
        fake_write_research_report,
    )
    monkeypatch.setattr(
        production_module,
        "build_research_reproducibility_audit",
        lambda *args: {"resource": {"peak_rss_bytes": 1, "peak_rss_exceeded": False}},
    )

    output = write_factor_production_training_report(result, tmp_path / "report")

    assert (output / "production_model.json").is_file()
    assert (output / "parameters.json").is_file()
    assert (output / "validation_selection.csv").is_file()
    assert (output / "training_summary.csv").is_file()
    assert (output / "production_targets.csv").is_file()
    assert (output / "summary.md").is_file()
    assert (output / "research_evaluation").is_dir()
