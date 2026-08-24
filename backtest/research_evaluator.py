import hashlib
import json
import math
import subprocess
import sys
import tomllib
from collections import Counter, OrderedDict
from collections.abc import MutableMapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    import resource
except ImportError:  # pragma: no cover - Windows does not provide resource
    resource = None

from backtest.config import BacktestConfig, load_backtest_config
from backtest.data_access import BacktestDataAccess
from backtest.factor_trainer import (
    FactorTrainingResult,
    fit_factor_weights,
    prepare_factor_training_data,
)
from backtest.hyperparameter_search import (
    HyperparameterSearchSpec,
    HyperparameterTrial,
    expand_hyperparameter_trials,
)
from backtest.metrics import calculate_performance_metrics
from backtest.research_provenance import (
    build_research_data_snapshot,
    verify_research_data_snapshot_unchanged,
)
from backtest.runner import BacktestExecutionCache, execute_backtest
from backtest.strategy_registry import get_backtest_strategy
from storage.database.manager import DBManager

SUPPORTED_SELECTION_METRICS = frozenset(
    {
        "total_return",
        "annualized_return",
        "annualized_volatility",
        "sharpe_ratio",
        "max_drawdown",
    }
)
SELECTION_DIRECTIONS = frozenset({"max", "min"})
REPORT_SCHEMA_VERSION = "1"
REPORT_SIGNAL_DATE_WARNING_THRESHOLD = 12
REPORT_TRAIN_VALIDATION_GAP_THRESHOLD = 0.25
REPORT_EXTREME_DRAWDOWN_THRESHOLD = -0.50
SELECTION_SCORE_TOLERANCE = 1e-12
DEFAULT_MINIMUM_TRAINING_SIGNAL_DATES = 24
DEFAULT_MINIMUM_VALIDATION_SIGNAL_DATES = 11
DEFAULT_MINIMUM_TEST_SIGNAL_DATES = 11
DEFAULT_MINIMUM_VALIDATION_OBSERVATIONS = 100
DEFAULT_MINIMUM_TEST_OBSERVATIONS = 100
DEFAULT_MAX_TRAINING_CACHE_ENTRIES = 4
DEFAULT_MINIMUM_EFFECTIVE_FACTOR_COUNT = 1.0
DEFAULT_MAXIMUM_FACTOR_WEIGHT = 1.0
DEFAULT_MAXIMUM_TRAINING_FAILURE_RATIO = 1.0
DEFAULT_MINIMUM_COMPLETED_TEST_WINDOWS = 1
# The data-loading stage is kept near 1.5 GiB; a complete evaluator process
# also retains two interval-level execution inputs and the active backtest
# structures. Use a separate 2 GiB process budget instead of conflating the
# two measurements.
RESEARCH_PEAK_RSS_LIMIT_BYTES = 2 * 1024**3
TRAINING_MODEL_COLUMNS = (
    "split_id",
    "candidate_id",
    "trial_id",
    "train_start_date",
    "train_end_date",
    "status",
    "error",
    "holding_count",
    "winsorize_lower",
    "winsorize_upper",
    "ridge_alpha",
    "portfolio_weighting",
    "factor_parameters",
    "factor_weights",
    "observation_count",
    "signal_date_count",
    "signal_date_start",
    "signal_date_end",
    "iterations",
    "converged",
    "label_horizon_days",
    "target_transform",
    "sample_weighting",
    "weight_constraint",
    "prior_factor_weights",
    "rebalance_frequency",
    "rebalance_interval_trading_days",
    "rebalance_anchor_date",
)
HYPERPARAMETER_TRIAL_COLUMNS = (
    "split_id",
    "candidate_id",
    "trial_id",
    "status",
    "error",
    "holding_count",
    "winsorize_lower",
    "winsorize_upper",
    "ridge_alpha",
    "portfolio_weighting",
    "factor_parameters",
    "factor_weights",
    "train_total_return",
    "train_annualized_return",
    "train_annualized_volatility",
    "train_sharpe_ratio",
    "train_max_drawdown",
    "train_trading_days",
    "train_target_observation_count",
    "train_signal_date_count",
    "validation_total_return",
    "validation_annualized_return",
    "validation_annualized_volatility",
    "validation_sharpe_ratio",
    "validation_max_drawdown",
    "validation_trading_days",
    "validation_target_observation_count",
    "validation_signal_date_count",
)
VALIDITY_COLUMNS = (
    "split_id",
    "candidate_id",
    "trial_id",
    "phase",
    "status",
    "error",
    "observation_count",
    "signal_date_count",
    "signal_date_start",
    "signal_date_end",
    "minimum_observations",
    "minimum_signal_dates",
    "effective_factor_count",
    "minimum_effective_factor_count",
    "maximum_factor_weight_actual",
    "maximum_factor_weight_allowed",
    "selection_score",
    "minimum_selection_score",
)
SELECTION_DIAGNOSTIC_COLUMNS = (
    "split_id",
    "status",
    "error",
    "candidate_count",
    "fitted_trial_count",
    "validation_signal_date_count",
    "validation_observation_count",
    "selected_candidate",
    "selected_trial_id",
    "selected_validation_score",
    "selected_rank",
    "second_validation_score",
    "selection_score_margin",
    "tied_best_trial_count",
    "validation_score_mean",
    "validation_score_std",
    "validation_score_median",
    "trials_per_validation_signal_date",
    "risk_level",
)
FACTOR_WEIGHT_DIAGNOSTIC_COLUMNS = (
    "split_id",
    "candidate_id",
    "trial_id",
    "status",
    "error",
    "selected",
    "factor_count",
    "nonzero_factor_count",
    "max_factor",
    "max_weight",
    "effective_factor_count",
    "weight_entropy",
    "normalized_weight_entropy",
    "collapse_level",
)
EVALUATION_FAILURE_COLUMNS = (
    "split_id",
    "candidate_id",
    "trial_id",
    "phase",
    "error",
)
PROTOCOL_COLUMNS = (
    "split_id",
    "gate",
    "status",
    "error",
    "actual_value",
    "required_value",
)


class BoundedTrainingDataCache:
    """Bound prepared training frames to one evaluation split."""

    def __init__(self, max_entries: int = DEFAULT_MAX_TRAINING_CACHE_ENTRIES):
        if isinstance(max_entries, bool) or not isinstance(max_entries, int):
            raise ValueError("max_training_cache_entries 必须是正整数")
        if max_entries <= 0:
            raise ValueError("max_training_cache_entries 必须是正整数")
        self.max_entries = max_entries
        self._entries: OrderedDict[tuple, pd.DataFrame] = OrderedDict()
        self._evictions = 0

    def __contains__(self, key: tuple) -> bool:
        return key in self._entries

    def __getitem__(self, key: tuple) -> pd.DataFrame:
        value = self._entries[key]
        self._entries.move_to_end(key)
        return value

    def __setitem__(self, key: tuple, value: pd.DataFrame) -> None:
        self.reserve_entry(key)
        self._entries[key] = value
        self._entries.move_to_end(key)

    def reserve_entry(self, key: tuple) -> None:
        """Evict before a cache miss allocates its replacement frame."""

        if key in self._entries:
            return
        while len(self._entries) >= self.max_entries:
            self._entries.popitem(last=False)
            self._evictions += 1

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def stats(self) -> dict[str, int]:
        return {
            "entries": len(self._entries),
            "max_entries": self.max_entries,
            "evictions": self._evictions,
        }


@dataclass(frozen=True)
class EvaluationPeriod:
    start_date: date
    end_date: date

    def __post_init__(self):
        if self.start_date >= self.end_date:
            raise ValueError("评估区间开始日期必须早于结束日期")

    def to_dict(self) -> dict[str, str]:
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }


@dataclass(frozen=True)
class EvaluationSplit:
    split_id: str
    train: EvaluationPeriod
    validation: EvaluationPeriod
    test: EvaluationPeriod

    def to_dict(self) -> dict:
        return {
            "split_id": self.split_id,
            "train": self.train.to_dict(),
            "validation": self.validation.to_dict(),
            "test": self.test.to_dict(),
        }


@dataclass(frozen=True)
class WalkForwardSpec:
    start_date: date
    end_date: date
    train_years: int
    validation_years: int
    test_years: int
    step_years: int

    def __post_init__(self):
        if self.start_date >= self.end_date:
            raise ValueError("Walk-forward 开始日期必须早于结束日期")
        for name in (
            "train_years",
            "validation_years",
            "test_years",
            "step_years",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"Walk-forward {name} 必须是正整数")

    def to_dict(self) -> dict:
        return {
            "enabled": True,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "train_years": self.train_years,
            "validation_years": self.validation_years,
            "test_years": self.test_years,
            "step_years": self.step_years,
        }


@dataclass(frozen=True)
class TrainingSpec:
    """Configuration for fitting factor weights inside each train window."""

    enabled: bool = True
    label_horizon_days: int = 20
    ridge_alpha: float = 0.1
    max_iterations: int = 5000
    minimum_training_observations: int = 200
    minimum_training_dates: int = DEFAULT_MINIMUM_TRAINING_SIGNAL_DATES
    max_training_cache_entries: int = DEFAULT_MAX_TRAINING_CACHE_ENTRIES
    refit_selected_on_train_validation: bool = False

    def __post_init__(self):
        if not isinstance(self.enabled, bool):
            raise ValueError("[training].enabled 必须是布尔值")
        if (
            isinstance(self.label_horizon_days, bool)
            or not isinstance(self.label_horizon_days, int)
            or self.label_horizon_days <= 0
        ):
            raise ValueError("[training].label_horizon_days 必须是正整数")
        if (
            isinstance(self.ridge_alpha, bool)
            or not isinstance(self.ridge_alpha, (int, float))
            or not math.isfinite(self.ridge_alpha)
            or self.ridge_alpha < 0
        ):
            raise ValueError("[training].ridge_alpha 必须是非负有限数字")
        for name in (
            "max_iterations",
            "minimum_training_observations",
            "minimum_training_dates",
            "max_training_cache_entries",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"[training].{name} 必须是正整数")
        if not isinstance(self.refit_selected_on_train_validation, bool):
            raise ValueError(
                "[training].refit_selected_on_train_validation 必须是布尔值"
            )

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "label_horizon_days": self.label_horizon_days,
            "ridge_alpha": self.ridge_alpha,
            "max_iterations": self.max_iterations,
            "minimum_training_observations": self.minimum_training_observations,
            "minimum_training_dates": self.minimum_training_dates,
            "max_training_cache_entries": self.max_training_cache_entries,
            "refit_selected_on_train_validation": (
                self.refit_selected_on_train_validation
            ),
        }


@dataclass(frozen=True)
class ResearchValiditySpec:
    """Predeclared sample, model, selection, and protocol gates."""

    enabled: bool = True
    minimum_training_signal_dates: int = DEFAULT_MINIMUM_TRAINING_SIGNAL_DATES
    minimum_validation_signal_dates: int = DEFAULT_MINIMUM_VALIDATION_SIGNAL_DATES
    minimum_test_signal_dates: int = DEFAULT_MINIMUM_TEST_SIGNAL_DATES
    minimum_validation_observations: int = DEFAULT_MINIMUM_VALIDATION_OBSERVATIONS
    minimum_test_observations: int = DEFAULT_MINIMUM_TEST_OBSERVATIONS
    minimum_effective_factor_count: float = DEFAULT_MINIMUM_EFFECTIVE_FACTOR_COUNT
    maximum_factor_weight: float = DEFAULT_MAXIMUM_FACTOR_WEIGHT
    minimum_validation_selection_score: float | None = None
    maximum_training_failure_ratio: float = DEFAULT_MAXIMUM_TRAINING_FAILURE_RATIO
    maximum_trials_per_validation_signal_date: float | None = None
    minimum_completed_test_windows: int = DEFAULT_MINIMUM_COMPLETED_TEST_WINDOWS
    require_non_overlapping_test_windows: bool = False

    def __post_init__(self):
        if not isinstance(self.enabled, bool):
            raise ValueError("[validity].enabled 必须是布尔值")
        for name in (
            "minimum_training_signal_dates",
            "minimum_validation_signal_dates",
            "minimum_test_signal_dates",
            "minimum_validation_observations",
            "minimum_test_observations",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"[validity].{name} 必须是正整数")
        for name in (
            "minimum_effective_factor_count",
            "maximum_factor_weight",
            "maximum_training_failure_ratio",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"[validity].{name} 必须是有限数字")
        if self.minimum_effective_factor_count < 1:
            raise ValueError("[validity].minimum_effective_factor_count 必须不小于 1")
        if not 0 < self.maximum_factor_weight <= 1:
            raise ValueError("[validity].maximum_factor_weight 必须在 (0, 1] 内")
        if not 0 <= self.maximum_training_failure_ratio <= 1:
            raise ValueError(
                "[validity].maximum_training_failure_ratio 必须在 [0, 1] 内"
            )
        for name in (
            "minimum_validation_selection_score",
            "maximum_trials_per_validation_signal_date",
        ):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"[validity].{name} 必须是有限数字或省略")
        if (
            self.maximum_trials_per_validation_signal_date is not None
            and self.maximum_trials_per_validation_signal_date <= 0
        ):
            raise ValueError(
                "[validity].maximum_trials_per_validation_signal_date 必须为正数"
            )
        if (
            isinstance(self.minimum_completed_test_windows, bool)
            or not isinstance(self.minimum_completed_test_windows, int)
            or self.minimum_completed_test_windows <= 0
        ):
            raise ValueError("[validity].minimum_completed_test_windows 必须是正整数")
        if not isinstance(self.require_non_overlapping_test_windows, bool):
            raise ValueError(
                "[validity].require_non_overlapping_test_windows 必须是布尔值"
            )

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "minimum_training_signal_dates": self.minimum_training_signal_dates,
            "minimum_validation_signal_dates": self.minimum_validation_signal_dates,
            "minimum_test_signal_dates": self.minimum_test_signal_dates,
            "minimum_validation_observations": self.minimum_validation_observations,
            "minimum_test_observations": self.minimum_test_observations,
            "minimum_effective_factor_count": self.minimum_effective_factor_count,
            "maximum_factor_weight": self.maximum_factor_weight,
            "minimum_validation_selection_score": (
                self.minimum_validation_selection_score
            ),
            "maximum_training_failure_ratio": self.maximum_training_failure_ratio,
            "maximum_trials_per_validation_signal_date": (
                self.maximum_trials_per_validation_signal_date
            ),
            "minimum_completed_test_windows": self.minimum_completed_test_windows,
            "require_non_overlapping_test_windows": (
                self.require_non_overlapping_test_windows
            ),
        }


class ResearchValidityError(ValueError):
    """Raised when a research phase cannot support a reliable conclusion."""


@dataclass(frozen=True)
class FactorExperimentEvaluationConfig:
    name: str
    candidate_configs: tuple[Path, ...]
    selection_metric: str
    selection_direction: str
    fixed_split: EvaluationSplit
    walk_forward: WalkForwardSpec | None = None
    training: TrainingSpec | None = None
    hyperparameter_search: HyperparameterSearchSpec | None = None
    validity: ResearchValiditySpec | None = None
    retrospective_method_development: bool = False

    def __post_init__(self):
        if not self.name:
            raise ValueError("研究评估名称不能为空")
        if not self.candidate_configs:
            raise ValueError("至少需要一个候选回测配置")
        if self.selection_metric not in SUPPORTED_SELECTION_METRICS:
            available = ", ".join(sorted(SUPPORTED_SELECTION_METRICS))
            raise ValueError(
                f"不支持的选择指标: {self.selection_metric} (可选: {available})"
            )
        if self.selection_direction not in SELECTION_DIRECTIONS:
            raise ValueError("selection_direction 必须是 max 或 min")
        if not isinstance(self.retrospective_method_development, bool):
            raise ValueError("retrospective_method_development 必须是布尔值")
        if (
            self.validity is not None
            and self.validity.enabled
            and self.validity.require_non_overlapping_test_windows
        ):
            _validate_non_overlapping_walk_forward_test_periods(self)

    def get_splits(self) -> tuple[EvaluationSplit, ...]:
        splits = [self.fixed_split]
        if self.walk_forward is not None:
            splits.extend(build_walk_forward_splits(self.walk_forward))
        return tuple(splits)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "candidate_configs": [str(path) for path in self.candidate_configs],
            "selection_metric": self.selection_metric,
            "selection_direction": self.selection_direction,
            "fixed_split": self.fixed_split.to_dict(),
            "walk_forward": (
                self.walk_forward.to_dict() if self.walk_forward else {"enabled": False}
            ),
            "training": (
                self.training.to_dict() if self.training else {"enabled": False}
            ),
            "hyperparameter_search": (
                self.hyperparameter_search.to_dict()
                if self.hyperparameter_search
                else {"enabled": False}
            ),
            "validity": (
                self.validity.to_dict() if self.validity else {"enabled": False}
            ),
            "retrospective_method_development": (self.retrospective_method_development),
        }


@dataclass(frozen=True)
class FactorExperimentEvaluationResult:
    config: FactorExperimentEvaluationConfig
    metric_rows: tuple[dict, ...]
    selection_rows: tuple[dict, ...]
    training_rows: tuple[dict, ...] = ()
    hyperparameter_rows: tuple[dict, ...] = ()
    evaluation_failure_rows: tuple[dict, ...] = ()
    validity_rows: tuple[dict, ...] = ()
    selection_diagnostic_rows: tuple[dict, ...] = ()
    protocol_rows: tuple[dict, ...] = ()
    protocol_status: str = "protocol_not_evaluated"
    strategy_evidence_status: str = "strategy_evidence_not_evaluated"
    data_snapshot: dict[str, object] | None = None


def load_factor_experiment_evaluation_config(
    config_path: str | Path,
) -> FactorExperimentEvaluationConfig:
    path = Path(config_path)
    with path.open("rb") as file:
        document = tomllib.load(file)

    experiment = document.get("experiment")
    split = document.get("split")
    if not isinstance(experiment, dict) or not isinstance(split, dict):
        raise ValueError("研究评估配置必须包含 [experiment] 和 [split] 区段")

    name = experiment.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("[experiment].name 必须是非空字符串")

    raw_candidates = experiment.get("candidate_configs")
    if not isinstance(raw_candidates, list) or not raw_candidates:
        raise ValueError("[experiment].candidate_configs 必须是非空数组")
    candidate_configs = []
    for raw_candidate in raw_candidates:
        if not isinstance(raw_candidate, str) or not raw_candidate:
            raise ValueError("候选回测配置路径必须是非空字符串")
        candidate_path = Path(raw_candidate)
        if not candidate_path.is_absolute():
            candidate_path = path.parent / candidate_path
        candidate_path = candidate_path.resolve()
        if not candidate_path.is_file():
            raise ValueError(f"候选回测配置不存在: {candidate_path}")
        candidate_configs.append(candidate_path)
    if len(set(candidate_configs)) != len(candidate_configs):
        raise ValueError("候选回测配置不能重复")

    selection_metric = experiment.get("selection_metric", "sharpe_ratio")
    if not isinstance(selection_metric, str):
        raise ValueError("selection_metric 必须是字符串")
    selection_direction = experiment.get("selection_direction", "max")
    if not isinstance(selection_direction, str):
        raise ValueError("selection_direction 必须是字符串")
    retrospective_method_development = experiment.get(
        "retrospective_method_development", False
    )
    if not isinstance(retrospective_method_development, bool):
        raise ValueError("[experiment].retrospective_method_development 必须是布尔值")

    fixed_split = EvaluationSplit(
        split_id="fixed_split",
        train=_parse_period(split, "train"),
        validation=_parse_period(split, "validation"),
        test=_parse_period(split, "test"),
    )
    _validate_split_order(fixed_split)

    walk_forward = _parse_walk_forward(document.get("walk_forward"))
    training = _parse_training(document.get("training"))
    hyperparameter_search = _parse_hyperparameter_search(
        document.get("hyperparameter_search")
    )
    validity = _parse_validity(document.get("validity"))
    return FactorExperimentEvaluationConfig(
        name=name,
        candidate_configs=tuple(candidate_configs),
        selection_metric=selection_metric,
        selection_direction=selection_direction,
        fixed_split=fixed_split,
        walk_forward=walk_forward,
        training=training,
        hyperparameter_search=hyperparameter_search,
        validity=validity,
        retrospective_method_development=retrospective_method_development,
    )


def build_walk_forward_splits(spec: WalkForwardSpec) -> tuple[EvaluationSplit, ...]:
    splits = []
    current_start = spec.start_date
    index = 1
    while True:
        train_start = current_start
        train_end = _add_years(train_start, spec.train_years) - timedelta(days=1)
        validation_start = train_end + timedelta(days=1)
        validation_end = _add_years(
            validation_start, spec.validation_years
        ) - timedelta(days=1)
        test_start = validation_end + timedelta(days=1)
        test_end = _add_years(test_start, spec.test_years) - timedelta(days=1)
        if test_end > spec.end_date:
            break
        splits.append(
            EvaluationSplit(
                split_id=f"walk_forward_{index:02d}",
                train=EvaluationPeriod(train_start, train_end),
                validation=EvaluationPeriod(validation_start, validation_end),
                test=EvaluationPeriod(test_start, test_end),
            )
        )
        index += 1
        current_start = _add_years(current_start, spec.step_years)
    if not splits:
        raise ValueError("Walk-forward 日期范围内没有完整的训练/验证/测试窗口")
    return tuple(splits)


def _validate_non_overlapping_walk_forward_test_periods(
    config: FactorExperimentEvaluationConfig,
) -> None:
    if config.walk_forward is None:
        return
    walk_forward_splits = build_walk_forward_splits(config.walk_forward)
    for previous, current in zip(
        walk_forward_splits, walk_forward_splits[1:], strict=False
    ):
        if previous.test.end_date >= current.test.start_date:
            raise ResearchValidityError(
                "研究协议门禁失败 [walk_forward_test_overlap]: "
                f"{previous.split_id} 测试结束 {previous.test.end_date} >= "
                f"{current.split_id} 测试开始 {current.test.start_date}"
            )


def evaluate_factor_experiments(
    config: FactorExperimentEvaluationConfig,
    database_manager: DBManager,
) -> FactorExperimentEvaluationResult:
    candidate_configs = [
        (path.stem, load_backtest_config(path)) for path in config.candidate_configs
    ]
    candidate_ids = [candidate_id for candidate_id, _ in candidate_configs]
    if len(set(candidate_ids)) != len(candidate_ids):
        raise ValueError("候选配置文件名必须唯一，以便稳定记录候选 ID")

    training_enabled = config.training is not None and config.training.enabled
    search_enabled = (
        config.hyperparameter_search is not None
        and config.hyperparameter_search.enabled
    )
    if search_enabled and not training_enabled:
        raise ValueError("启用超参数搜索必须同时启用 [training]")
    if search_enabled:
        trials = expand_hyperparameter_trials(
            candidate_configs,
            config.hyperparameter_search,
        )
    else:
        trials = tuple(
            HyperparameterTrial(
                trial_id=candidate_id,
                candidate_id=candidate_id,
                config=candidate_config,
                parameters=(
                    _build_default_trial_parameters(candidate_config, config.training)
                    if training_enabled
                    else {}
                ),
            )
            for candidate_id, candidate_config in candidate_configs
        )

    metric_rows = []
    selection_rows = []
    training_rows = []
    hyperparameter_rows = []
    evaluation_failure_rows = []
    validity_rows = []
    selection_diagnostic_rows = []
    protocol_rows = []
    if (
        config.validity is not None
        and config.validity.enabled
        and config.validity.require_non_overlapping_test_windows
    ):
        protocol_rows.append(
            _build_protocol_row(
                split_id="all_walk_forward",
                gate="non_overlapping_test_windows",
                status="passed",
                actual_value=True,
                required_value=True,
            )
        )
    data_snapshot = build_research_data_snapshot(database_manager)
    for split in config.get_splits():
        observed_validation_scores = {}
        validation_scores = {}
        validation_coverage = {}
        effective_configs = {}
        validation_rejection_count = 0
        model_rejection_count = 0
        training_fit_failure_count = 0
        trial_map = {trial.trial_id: trial for trial in trials}
        training_data_cache = (
            BoundedTrainingDataCache(config.training.max_training_cache_entries)
            if search_enabled and config.training is not None
            else None
        )
        # Research runs may contain many trial-specific weight maps, but a
        # split only needs the train and validation factor inputs. Keeping
        # those two periods avoids reloading the same candidates for every
        # trial without retaining test data across the split.
        execution_cache = BacktestExecutionCache(
            max_entries=2,
            market_data_max_entries=1,
        )
        for trial in trials:
            effective_config = trial.config
            training_result = None
            training_fit_completed = False
            if training_enabled:
                try:
                    if search_enabled:
                        effective_config, training_result = _fit_candidate_config(
                            trial.config,
                            split.train,
                            config.training,
                            database_manager,
                            ridge_alpha=trial.parameters["ridge_alpha"],
                            training_data_cache=training_data_cache,
                        )
                    else:
                        effective_config, training_result = _fit_candidate_config(
                            trial.config,
                            split.train,
                            config.training,
                            database_manager,
                        )
                    training_fit_completed = True
                    _append_training_validity_check(
                        validity_rows,
                        split,
                        trial,
                        training_result,
                        config.validity,
                        config.training,
                    )
                except Exception as error:
                    if not training_fit_completed:
                        training_fit_failure_count += 1
                    elif isinstance(error, ResearchValidityError):
                        model_rejection_count += 1
                    if (
                        config.validity is not None
                        and config.validity.enabled
                        and not training_fit_completed
                    ):
                        validity_rows.append(
                            _build_training_validity_row(
                                split,
                                trial,
                                training_result,
                                status="failed",
                                error=str(error),
                                training=config.training,
                                validity=config.validity,
                            )
                        )
                    failed_training_row = _build_failed_training_row(
                        split,
                        trial,
                        error,
                        training_result=training_result,
                    )
                    if training_fit_completed:
                        failed_training_row["status"] = "rejected"
                    training_rows.append(failed_training_row)
                    if search_enabled:
                        hyperparameter_rows.append(
                            _build_hyperparameter_trial_row(
                                split,
                                trial,
                                status=(
                                    "rejected" if training_fit_completed else "failed"
                                ),
                                error=str(error),
                                factor_weights=(
                                    training_result.factor_weights
                                    if training_result is not None
                                    else None
                                ),
                            )
                        )
                    continue
            effective_configs[trial.trial_id] = effective_config
            train_metrics = None
            validation_metrics = None
            rejection_error = ""
            phase_name = "train"
            try:
                for phase_name, period in (
                    ("train", split.train),
                    ("validation", split.validation),
                ):
                    run = execute_backtest(
                        _replace_backtest_period(effective_config, period),
                        database_manager,
                        include_benchmark=False,
                        execution_cache=execution_cache,
                    )
                    metrics = calculate_performance_metrics(run.result.daily_nav)
                    coverage = _summarize_target_coverage(
                        run.targets,
                        period,
                        run.result.daily_nav,
                    )
                    metric_rows.append(
                        _build_metric_row(
                            split,
                            phase_name,
                            trial.candidate_id,
                            run.config,
                            period,
                            metrics,
                            coverage=coverage,
                            trial_id=trial.trial_id if search_enabled else None,
                        )
                    )
                    if phase_name == "validation":
                        _append_backtest_validity_check(
                            validity_rows,
                            split,
                            trial,
                            phase_name,
                            coverage,
                            config.validity,
                        )
                    selection_metric_value = _require_finite_selection_metric(
                        metrics,
                        config.selection_metric,
                        phase_name,
                    )
                    if phase_name == "validation":
                        validation_metrics = {**metrics, **coverage}
                        validation_coverage[trial.trial_id] = coverage
                        observed_validation_scores[trial.trial_id] = (
                            selection_metric_value
                        )
                        rejection_error = _append_validation_selection_score_check(
                            validity_rows,
                            split,
                            trial,
                            selection_metric_value,
                            config.validity,
                        )
                        if rejection_error:
                            validation_rejection_count += 1
                        else:
                            validation_scores[trial.trial_id] = selection_metric_value
                    else:
                        train_metrics = {**metrics, **coverage}
            except Exception as error:
                evaluation_failure_rows.append(
                    _build_evaluation_failure_row(
                        split,
                        trial,
                        phase_name,
                        error,
                    )
                )
                if training_enabled:
                    training_rows.append(
                        _build_training_row(
                            split,
                            trial,
                            training_result,
                        )
                    )
                if search_enabled:
                    hyperparameter_rows.append(
                        _build_hyperparameter_trial_row(
                            split,
                            trial,
                            status="failed",
                            error=str(error),
                            factor_weights=(
                                training_result.factor_weights
                                if training_result is not None
                                else None
                            ),
                        )
                    )
                continue
            if training_enabled:
                training_rows.append(
                    _build_training_row(
                        split,
                        trial,
                        training_result,
                    )
                )
            if search_enabled:
                hyperparameter_rows.append(
                    _build_hyperparameter_trial_row(
                        split,
                        trial,
                        status=(
                            "rejected"
                            if trial.trial_id not in validation_scores
                            else "fitted"
                        ),
                        error=rejection_error or "",
                        train_metrics=train_metrics,
                        validation_metrics=validation_metrics,
                        factor_weights=training_result.factor_weights,
                    )
                )

        if training_data_cache is not None:
            # All trial fits are complete before validation selection starts;
            # no training-wide frame is needed by the backtest phases.
            training_data_cache.clear()
        diagnostic_validation_coverage = next(iter(validation_coverage.values()), None)
        protocol_error = _append_split_protocol_checks(
            protocol_rows,
            split,
            trial_count=len(trials),
            training_fit_failure_count=training_fit_failure_count,
            validation_coverage=validation_coverage,
            validity=config.validity,
        )
        if protocol_error:
            selection_diagnostic_rows.append(
                _build_selection_diagnostic_row(
                    split,
                    observed_validation_scores,
                    trial_map,
                    selected_trial_id=None,
                    selected_score=None,
                    validation_coverage=diagnostic_validation_coverage,
                    status="rejected",
                    error=protocol_error,
                    selection_direction=config.selection_direction,
                )
            )
            selection_rows.append(
                {
                    "split_id": split.split_id,
                    "status": "rejected",
                    "error": protocol_error,
                    "selected_candidate": "",
                    "selected_trial_id": "",
                    "validation_score": None,
                }
            )
            execution_cache.clear()
            continue
        try:
            selected_trial_id, selected_score = _select_candidate(
                validation_scores,
                config.selection_direction,
                split.split_id,
            )
        except ValueError as error:
            rejection_status = (
                "rejected"
                if validation_rejection_count or model_rejection_count
                else "failed"
            )
            selection_error = str(error)
            if rejection_status == "rejected":
                selection_error = (
                    f"{split.split_id} 的所有候选均被预设模型或验证门禁拒绝，"
                    "未运行测试段"
                )
            selection_diagnostic_rows.append(
                _build_selection_diagnostic_row(
                    split,
                    observed_validation_scores,
                    trial_map,
                    selected_trial_id=None,
                    selected_score=None,
                    validation_coverage=diagnostic_validation_coverage,
                    status=rejection_status,
                    error=selection_error,
                    selection_direction=config.selection_direction,
                )
            )
            selection_rows.append(
                {
                    "split_id": split.split_id,
                    "status": rejection_status,
                    "error": selection_error,
                    "selected_candidate": "",
                    "selected_trial_id": "",
                    "validation_score": None,
                }
            )
            if training_data_cache is not None:
                training_data_cache.clear()
            execution_cache.clear()
            continue
        selected_trial = trial_map[selected_trial_id]
        selected_config = effective_configs[selected_trial_id]
        test_refit_period = None
        test_refit_result = None
        if (
            training_enabled
            and config.training is not None
            and config.training.refit_selected_on_train_validation
        ):
            test_refit_period = EvaluationPeriod(
                split.train.start_date,
                split.validation.end_date,
            )
            try:
                if search_enabled:
                    selected_config, test_refit_result = _fit_candidate_config(
                        selected_trial.config,
                        test_refit_period,
                        config.training,
                        database_manager,
                        ridge_alpha=selected_trial.parameters["ridge_alpha"],
                    )
                else:
                    selected_config, test_refit_result = _fit_candidate_config(
                        selected_trial.config,
                        test_refit_period,
                        config.training,
                        database_manager,
                    )
                if config.validity is not None and config.validity.enabled:
                    validate_factor_training_result(
                        test_refit_result,
                        config.training,
                        config.validity,
                    )
            except Exception as error:
                evaluation_failure_rows.append(
                    _build_evaluation_failure_row(
                        split,
                        selected_trial,
                        "test_refit",
                        error,
                    )
                )
                selection_rows.append(
                    _build_selection_row(
                        split,
                        selected_trial,
                        selected_score,
                        status="failed",
                        error=str(error),
                        training_enabled=training_enabled,
                        training=config.training,
                        effective_config=effective_configs[selected_trial_id],
                        validation_coverage=validation_coverage.get(selected_trial_id),
                        test_refit_period=test_refit_period,
                    )
                )
                execution_cache.clear()
                continue
        selection_diagnostic_rows.append(
            _build_selection_diagnostic_row(
                split,
                observed_validation_scores,
                trial_map,
                selected_trial_id=selected_trial_id,
                selected_score=selected_score,
                validation_coverage=validation_coverage.get(selected_trial_id),
                status="completed",
                error="",
                selection_direction=config.selection_direction,
            )
        )
        test_period = split.test
        try:
            test_run = execute_backtest(
                _replace_backtest_period(selected_config, test_period),
                database_manager,
                include_benchmark=False,
                execution_cache=execution_cache,
            )
            test_coverage = _summarize_target_coverage(
                test_run.targets,
                test_period,
                test_run.result.daily_nav,
            )
            _append_backtest_validity_check(
                validity_rows,
                split,
                selected_trial,
                "test",
                test_coverage,
                config.validity,
            )
            test_metrics = calculate_performance_metrics(test_run.result.daily_nav)
            _require_finite_selection_metric(
                test_metrics,
                config.selection_metric,
                "test",
            )
        except Exception as error:
            evaluation_failure_rows.append(
                _build_evaluation_failure_row(split, selected_trial, "test", error)
            )
            selection_rows.append(
                _build_selection_row(
                    split,
                    selected_trial,
                    selected_score,
                    status="failed",
                    error=str(error),
                    training_enabled=training_enabled,
                    training=config.training,
                    effective_config=selected_config,
                    validation_coverage=validation_coverage.get(selected_trial_id),
                    test_refit_period=test_refit_period,
                    test_refit_result=test_refit_result,
                )
            )
            if training_data_cache is not None:
                training_data_cache.clear()
            execution_cache.clear()
            continue
        metric_rows.append(
            _build_metric_row(
                split,
                "test",
                selected_trial.candidate_id,
                test_run.config,
                test_period,
                test_metrics,
                coverage=test_coverage,
                trial_id=selected_trial_id if search_enabled else None,
            )
        )
        selection_rows.append(
            _build_selection_row(
                split,
                selected_trial,
                selected_score,
                status="completed",
                error="",
                training_enabled=training_enabled,
                training=config.training,
                effective_config=selected_config,
                test_metrics=test_metrics,
                validation_coverage=validation_coverage.get(selected_trial_id),
                test_coverage=test_coverage,
                test_refit_period=test_refit_period,
                test_refit_result=test_refit_result,
            )
        )
        if training_data_cache is not None:
            training_data_cache.clear()
        execution_cache.clear()

    completed_test_selections = [
        row
        for row in selection_rows
        if row.get("status") == "completed"
        and (
            config.walk_forward is None
            or str(row.get("split_id", "")).startswith("walk_forward_")
        )
    ]
    if config.validity is not None and config.validity.enabled:
        minimum_completed_test_windows = config.validity.minimum_completed_test_windows
        completed_window_count = len(completed_test_selections)
        completed_status = (
            "passed"
            if completed_window_count >= minimum_completed_test_windows
            else "failed"
        )
        completed_error = ""
        if completed_status == "failed":
            completed_error = (
                "研究协议门禁失败 [minimum_completed_test_windows]: "
                f"{completed_window_count} < {minimum_completed_test_windows}"
            )
        protocol_rows.append(
            _build_protocol_row(
                split_id="all_walk_forward" if config.walk_forward else "all_splits",
                gate="minimum_completed_test_windows",
                status=completed_status,
                error=completed_error,
                actual_value=completed_window_count,
                required_value=minimum_completed_test_windows,
            )
        )
    protocol_status = (
        "protocol_failed"
        if any(row.get("status") != "passed" for row in protocol_rows)
        else "protocol_passed"
        if config.validity is not None and config.validity.enabled
        else "protocol_not_evaluated"
    )
    strategy_evidence_status = _determine_strategy_evidence_status(
        config,
        protocol_status,
        completed_test_selections,
    )
    data_snapshot = verify_research_data_snapshot_unchanged(
        data_snapshot,
        build_research_data_snapshot(database_manager),
    )
    return FactorExperimentEvaluationResult(
        config=config,
        metric_rows=tuple(metric_rows),
        selection_rows=tuple(selection_rows),
        training_rows=tuple(training_rows),
        hyperparameter_rows=tuple(hyperparameter_rows),
        evaluation_failure_rows=tuple(evaluation_failure_rows),
        validity_rows=tuple(validity_rows),
        selection_diagnostic_rows=tuple(selection_diagnostic_rows),
        protocol_rows=tuple(protocol_rows),
        protocol_status=protocol_status,
        strategy_evidence_status=strategy_evidence_status,
        data_snapshot=data_snapshot,
    )


def write_factor_experiment_evaluation_report(
    result: FactorExperimentEvaluationResult,
    output_dir: str | Path,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    audit = _build_reproducibility_audit(result.config, result.data_snapshot)
    (output_path / "parameters.json").write_text(
        json.dumps(
            {
                **result.config.to_dict(),
                "protocol_status": result.protocol_status,
                "strategy_evidence_status": result.strategy_evidence_status,
                "audit": audit,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(result.metric_rows).to_csv(
        output_path / "candidate_metrics.csv", index=False
    )
    pd.DataFrame(result.selection_rows).to_csv(
        output_path / "selections.csv", index=False
    )
    pd.DataFrame(result.training_rows, columns=TRAINING_MODEL_COLUMNS).to_csv(
        output_path / "training_models.csv", index=False
    )
    pd.DataFrame(
        result.hyperparameter_rows,
        columns=HYPERPARAMETER_TRIAL_COLUMNS,
    ).to_csv(output_path / "hyperparameter_trials.csv", index=False)
    pd.DataFrame(
        result.evaluation_failure_rows,
        columns=EVALUATION_FAILURE_COLUMNS,
    ).to_csv(output_path / "evaluation_failures.csv", index=False)
    pd.DataFrame(result.validity_rows, columns=VALIDITY_COLUMNS).to_csv(
        output_path / "research_validity.csv", index=False
    )
    pd.DataFrame(result.protocol_rows, columns=PROTOCOL_COLUMNS).to_csv(
        output_path / "research_protocol.csv", index=False
    )
    pd.DataFrame(
        result.selection_diagnostic_rows,
        columns=SELECTION_DIAGNOSTIC_COLUMNS,
    ).to_csv(output_path / "selection_diagnostics.csv", index=False)
    pd.DataFrame(
        _build_factor_weight_diagnostic_rows(result),
        columns=FACTOR_WEIGHT_DIAGNOSTIC_COLUMNS,
    ).to_csv(output_path / "factor_weight_diagnostics.csv", index=False)
    _write_evaluation_summary(output_path / "summary.md", result, audit)
    return output_path


def _parse_period(section: dict, prefix: str) -> EvaluationPeriod:
    start_key = f"{prefix}_start_date"
    end_key = f"{prefix}_end_date"
    try:
        start_date = date.fromisoformat(section[start_key])
        end_date = date.fromisoformat(section[end_key])
    except KeyError as error:
        raise ValueError(f"[split] 缺少字段: {error.args[0]}") from error
    except (TypeError, ValueError) as error:
        raise ValueError(f"[split] {prefix} 日期必须使用 YYYY-MM-DD 格式") from error
    return EvaluationPeriod(start_date, end_date)


def _parse_walk_forward(section: object) -> WalkForwardSpec | None:
    if section is None:
        return None
    if not isinstance(section, dict):
        raise ValueError("[walk_forward] 必须是 TOML 表")
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("[walk_forward].enabled 必须是布尔值")
    if not enabled:
        return None
    try:
        start_date = date.fromisoformat(section["start_date"])
        end_date = date.fromisoformat(section["end_date"])
    except KeyError as error:
        raise ValueError(f"[walk_forward] 缺少字段: {error.args[0]}") from error
    except (TypeError, ValueError) as error:
        raise ValueError("[walk_forward] 日期必须使用 YYYY-MM-DD 格式") from error
    try:
        return WalkForwardSpec(
            start_date=start_date,
            end_date=end_date,
            train_years=section["train_years"],
            validation_years=section["validation_years"],
            test_years=section["test_years"],
            step_years=section["step_years"],
        )
    except KeyError as error:
        raise ValueError(f"[walk_forward] 缺少字段: {error.args[0]}") from error


def _parse_training(section: object) -> TrainingSpec | None:
    if section is None:
        return None
    if not isinstance(section, dict):
        raise ValueError("[training] 必须是 TOML 表")
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("[training].enabled 必须是布尔值")
    if not enabled:
        return None
    return TrainingSpec(
        enabled=True,
        label_horizon_days=section.get("label_horizon_days", 20),
        ridge_alpha=section.get("ridge_alpha", 0.1),
        max_iterations=section.get("max_iterations", 5000),
        minimum_training_observations=section.get("minimum_training_observations", 200),
        minimum_training_dates=section.get(
            "minimum_training_dates", DEFAULT_MINIMUM_TRAINING_SIGNAL_DATES
        ),
        max_training_cache_entries=section.get(
            "max_training_cache_entries", DEFAULT_MAX_TRAINING_CACHE_ENTRIES
        ),
        refit_selected_on_train_validation=section.get(
            "refit_selected_on_train_validation", False
        ),
    )


def _parse_validity(section: object) -> ResearchValiditySpec:
    if section is None:
        return ResearchValiditySpec()
    if not isinstance(section, dict):
        raise ValueError("[validity] 必须是 TOML 表")
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("[validity].enabled 必须是布尔值")
    if not enabled:
        return ResearchValiditySpec(enabled=False)
    return ResearchValiditySpec(
        enabled=True,
        minimum_training_signal_dates=section.get(
            "minimum_training_signal_dates", DEFAULT_MINIMUM_TRAINING_SIGNAL_DATES
        ),
        minimum_validation_signal_dates=section.get(
            "minimum_validation_signal_dates", DEFAULT_MINIMUM_VALIDATION_SIGNAL_DATES
        ),
        minimum_test_signal_dates=section.get(
            "minimum_test_signal_dates", DEFAULT_MINIMUM_TEST_SIGNAL_DATES
        ),
        minimum_validation_observations=section.get(
            "minimum_validation_observations", DEFAULT_MINIMUM_VALIDATION_OBSERVATIONS
        ),
        minimum_test_observations=section.get(
            "minimum_test_observations", DEFAULT_MINIMUM_TEST_OBSERVATIONS
        ),
        minimum_effective_factor_count=section.get(
            "minimum_effective_factor_count",
            DEFAULT_MINIMUM_EFFECTIVE_FACTOR_COUNT,
        ),
        maximum_factor_weight=section.get(
            "maximum_factor_weight", DEFAULT_MAXIMUM_FACTOR_WEIGHT
        ),
        minimum_validation_selection_score=section.get(
            "minimum_validation_selection_score"
        ),
        maximum_training_failure_ratio=section.get(
            "maximum_training_failure_ratio",
            DEFAULT_MAXIMUM_TRAINING_FAILURE_RATIO,
        ),
        maximum_trials_per_validation_signal_date=section.get(
            "maximum_trials_per_validation_signal_date"
        ),
        minimum_completed_test_windows=section.get(
            "minimum_completed_test_windows",
            DEFAULT_MINIMUM_COMPLETED_TEST_WINDOWS,
        ),
        require_non_overlapping_test_windows=section.get(
            "require_non_overlapping_test_windows", False
        ),
    )


def _parse_hyperparameter_search(
    section: object,
) -> HyperparameterSearchSpec | None:
    if section is None:
        return None
    if not isinstance(section, dict):
        raise ValueError("[hyperparameter_search] 必须是 TOML 表")
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("[hyperparameter_search].enabled 必须是布尔值")
    if not enabled:
        return None
    raw_factor_parameters = section.get("factor_parameters", {})
    if not isinstance(raw_factor_parameters, dict):
        raise ValueError("[hyperparameter_search.factor_parameters] 必须是 TOML 表")
    factor_parameter_values = {}
    for factor_name, raw_parameters in raw_factor_parameters.items():
        if not isinstance(raw_parameters, dict):
            raise ValueError(
                f"[hyperparameter_search.factor_parameters.{factor_name}] 必须是 TOML 表"
            )
        factor_parameter_values[factor_name] = {}
        for parameter_name, values in raw_parameters.items():
            if isinstance(values, (str, bytes)) or not isinstance(values, list):
                raise ValueError(
                    "超参数搜索的因子参数值必须是 TOML 数组: "
                    f"{factor_name}.{parameter_name}"
                )
            factor_parameter_values[factor_name][parameter_name] = tuple(values)
    return HyperparameterSearchSpec(
        enabled=True,
        max_combinations=section.get("max_combinations", 100),
        holding_counts=_parse_search_array(section, "holding_counts", (20,)),
        winsorize_ranges=tuple(
            tuple(value)
            for value in _parse_search_array(
                section, "winsorize_ranges", ((0.05, 0.95),)
            )
        ),
        ridge_alphas=_parse_search_array(section, "ridge_alphas", (0.1,)),
        factor_parameter_values=factor_parameter_values,
    )


def _parse_search_array(section: dict, key: str, default: tuple) -> tuple:
    value = section.get(key, default)
    if isinstance(value, (str, bytes)) or not isinstance(value, list | tuple):
        raise ValueError(f"[hyperparameter_search].{key} 必须是 TOML 数组")
    return tuple(value)


def _validate_split_order(split: EvaluationSplit) -> None:
    if split.train.end_date >= split.validation.start_date:
        raise ValueError("训练集与验证集日期不能重叠")
    if split.validation.end_date >= split.test.start_date:
        raise ValueError("验证集与测试集日期不能重叠")


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        # 处理从闰日开始的滚动窗口，保持自然年语义并落到 2 月 28 日。
        return value.replace(year=value.year + years, day=28)


def _replace_backtest_period(
    config: BacktestConfig, period: EvaluationPeriod
) -> BacktestConfig:
    return replace(
        config,
        start_date=period.start_date,
        end_date=period.end_date,
        strategy_version="",
    )


def _build_default_trial_parameters(
    candidate_config: BacktestConfig, training: TrainingSpec | None
) -> dict[str, object]:
    resolved = get_backtest_strategy(
        candidate_config.strategy_name
    ).validate_parameters(candidate_config.strategy_parameters)
    return {
        "holding_count": resolved.get("holding_count"),
        "winsorize_lower": resolved.get("winsorize_lower"),
        "winsorize_upper": resolved.get("winsorize_upper"),
        "ridge_alpha": training.ridge_alpha if training is not None else None,
        "factor_parameters": resolved.get("factor_parameters", {}),
    }


def _build_selection_row(
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    selected_score: float,
    *,
    status: str,
    error: str,
    training_enabled: bool,
    training: TrainingSpec | None,
    effective_config: BacktestConfig,
    test_metrics: dict | None = None,
    validation_coverage: dict | None = None,
    test_coverage: dict | None = None,
    test_refit_period: EvaluationPeriod | None = None,
    test_refit_result: FactorTrainingResult | None = None,
) -> dict:
    try:
        resolved = get_backtest_strategy(
            effective_config.strategy_name
        ).validate_parameters(effective_config.strategy_parameters)
    except (KeyError, ValueError):
        # 评估器的单元测试可以用未注册的 fake strategy；生产候选在加载阶段已校验。
        resolved = dict(effective_config.strategy_parameters)
    trial_parameters = trial.parameters
    ridge_alpha = trial_parameters.get("ridge_alpha")
    if ridge_alpha is None and training_enabled and training is not None:
        ridge_alpha = training.ridge_alpha
    factor_parameters = trial_parameters.get(
        "factor_parameters", resolved.get("factor_parameters", {})
    )
    metrics = test_metrics or {}
    validation_coverage = validation_coverage or {}
    test_coverage = test_coverage or {}
    return {
        "split_id": split.split_id,
        "status": status,
        "error": error,
        "train_start_date": split.train.start_date.isoformat(),
        "train_end_date": split.train.end_date.isoformat(),
        "validation_start_date": split.validation.start_date.isoformat(),
        "validation_end_date": split.validation.end_date.isoformat(),
        "test_start_date": split.test.start_date.isoformat(),
        "test_end_date": split.test.end_date.isoformat(),
        "selected_candidate": trial.candidate_id,
        "selected_trial_id": trial.trial_id,
        "validation_score": selected_score,
        "validation_target_observation_count": validation_coverage.get(
            "target_observation_count"
        ),
        "validation_signal_date_count": validation_coverage.get("signal_date_count"),
        "selected_holding_count": resolved.get("holding_count"),
        "selected_winsorize_lower": resolved.get("winsorize_lower"),
        "selected_winsorize_upper": resolved.get("winsorize_upper"),
        "selected_ridge_alpha": ridge_alpha,
        "selected_portfolio_weighting": resolved.get("portfolio_weighting", "equal"),
        "selected_factor_parameters": json.dumps(
            factor_parameters, ensure_ascii=False, sort_keys=True
        ),
        "selected_factor_weights": json.dumps(
            resolved.get("factor_weights", {}), ensure_ascii=False, sort_keys=True
        ),
        "test_refit_performed": test_refit_result is not None,
        "test_refit_train_start_date": (
            test_refit_period.start_date.isoformat() if test_refit_period else None
        ),
        "test_refit_train_end_date": (
            test_refit_period.end_date.isoformat() if test_refit_period else None
        ),
        "test_refit_observation_count": (
            test_refit_result.observation_count if test_refit_result else None
        ),
        "test_refit_signal_date_count": (
            test_refit_result.signal_date_count if test_refit_result else None
        ),
        "test_refit_signal_date_start": (
            test_refit_result.signal_date_start if test_refit_result else None
        ),
        "test_refit_signal_date_end": (
            test_refit_result.signal_date_end if test_refit_result else None
        ),
        "test_total_return": metrics.get("total_return"),
        "test_annualized_return": metrics.get("annualized_return"),
        "test_annualized_volatility": metrics.get("annualized_volatility"),
        "test_sharpe_ratio": metrics.get("sharpe_ratio"),
        "test_max_drawdown": metrics.get("max_drawdown"),
        "test_trading_days": metrics.get("trading_days"),
        "test_target_observation_count": test_coverage.get("target_observation_count"),
        "test_signal_date_count": test_coverage.get("signal_date_count"),
    }


def _build_selection_diagnostic_row(
    split: EvaluationSplit,
    validation_scores: dict[str, float],
    trial_map: dict[str, HyperparameterTrial],
    *,
    selected_trial_id: str | None,
    selected_score: float | None,
    validation_coverage: dict | None,
    status: str,
    error: str,
    selection_direction: str,
) -> dict:
    valid_scores = [
        (trial_id, float(score))
        for trial_id, score in validation_scores.items()
        if score is not None and math.isfinite(float(score))
    ]
    valid_scores.sort(
        key=lambda item: item[1],
        reverse=selection_direction == "max",
    )
    score_values = [score for _, score in valid_scores]
    selected_rank = next(
        (
            index
            for index, (trial_id, _) in enumerate(valid_scores, start=1)
            if trial_id == selected_trial_id
        ),
        None,
    )
    top_score = valid_scores[0][1] if valid_scores else None
    second_score = valid_scores[1][1] if len(valid_scores) > 1 else None
    if top_score is None or second_score is None:
        score_margin = None
    elif selection_direction == "max":
        score_margin = top_score - second_score
    else:
        score_margin = second_score - top_score
    tied_best_count = (
        sum(
            math.isclose(
                score,
                top_score,
                rel_tol=0.0,
                abs_tol=SELECTION_SCORE_TOLERANCE,
            )
            for score in score_values
        )
        if top_score is not None
        else 0
    )
    coverage = validation_coverage or {}
    signal_date_count = _coerce_finite_number(coverage.get("signal_date_count"))
    observation_count = _coerce_finite_number(coverage.get("target_observation_count"))
    trials_per_signal_date = (
        len(valid_scores) / signal_date_count
        if signal_date_count is not None and signal_date_count > 0
        else None
    )
    candidate_count = len(
        {
            trial_map[trial_id].candidate_id
            for trial_id, _ in valid_scores
            if trial_id in trial_map
        }
    )
    selected_candidate = (
        trial_map[selected_trial_id].candidate_id
        if selected_trial_id in trial_map
        else None
    )
    return {
        "split_id": split.split_id,
        "status": status,
        "error": error,
        "candidate_count": candidate_count,
        "fitted_trial_count": len(valid_scores),
        "validation_signal_date_count": signal_date_count,
        "validation_observation_count": observation_count,
        "selected_candidate": selected_candidate,
        "selected_trial_id": selected_trial_id,
        "selected_validation_score": selected_score,
        "selected_rank": selected_rank,
        "second_validation_score": second_score,
        "selection_score_margin": score_margin,
        "tied_best_trial_count": tied_best_count,
        "validation_score_mean": _mean(score_values),
        "validation_score_std": _population_std(score_values),
        "validation_score_median": _median(score_values),
        "trials_per_validation_signal_date": trials_per_signal_date,
        "risk_level": _get_selection_risk_level(len(valid_scores), signal_date_count),
    }


def _get_selection_risk_level(
    fitted_trial_count: int, validation_signal_date_count: float | None
) -> str:
    if fitted_trial_count <= 0 or validation_signal_date_count is None:
        return "not_available"
    if (
        validation_signal_date_count <= REPORT_SIGNAL_DATE_WARNING_THRESHOLD
        and fitted_trial_count > 1
    ):
        return "high"
    if fitted_trial_count > validation_signal_date_count:
        return "elevated"
    return "not_flagged"


def _build_evaluation_failure_row(
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    phase: str,
    error: Exception,
) -> dict:
    return {
        "split_id": split.split_id,
        "candidate_id": trial.candidate_id,
        "trial_id": trial.trial_id,
        "phase": phase,
        "error": f"{type(error).__name__}: {error}",
    }


def _fit_candidate_config(
    candidate_config: BacktestConfig,
    train_period: EvaluationPeriod,
    training: TrainingSpec,
    database_manager: DBManager,
    *,
    ridge_alpha: float | None = None,
    training_data_cache: MutableMapping | None = None,
) -> tuple[BacktestConfig, FactorTrainingResult]:
    strategy = get_backtest_strategy(candidate_config.strategy_name)
    if not getattr(strategy, "supports_factor_training", False):
        raise ValueError(
            f"策略未声明正式因子权重训练能力: {candidate_config.strategy_name}"
        )

    resolved_parameters = strategy.validate_parameters(
        candidate_config.strategy_parameters
    )
    factor_names = tuple(resolved_parameters["factor_weights"])
    factor_parameters = resolved_parameters.get("factor_parameters", {})
    training_config = _replace_backtest_period(candidate_config, train_period)
    label_data_end_date = train_period.end_date + timedelta(
        days=training.label_horizon_days * 3 + 30
    )
    cache_key = (
        candidate_config.strategy_name,
        factor_names,
        json.dumps(
            factor_parameters,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        ),
        resolved_parameters["min_listing_days"],
        train_period.start_date,
        train_period.end_date,
        training.label_horizon_days,
        training_config.rebalance_frequency,
        training_config.rebalance_interval_trading_days,
    )
    if training_data_cache is not None and cache_key in training_data_cache:
        training_data = training_data_cache[cache_key]
    else:
        reserve_entry = getattr(training_data_cache, "reserve_entry", None)
        if callable(reserve_entry):
            reserve_entry(cache_key)
        raw_training_data = BacktestDataAccess(database_manager).load_factor_data(
            training_config,
            factor_names,
            factor_parameters=factor_parameters,
            minimum_history_days=resolved_parameters["min_listing_days"],
            data_end_date=label_data_end_date,
            financial_signal_dates_only=True,
        )
        training_data = prepare_factor_training_data(
            raw_training_data,
            factor_names,
            factor_parameters,
            train_period.start_date,
            train_period.end_date,
            minimum_history_days=resolved_parameters["min_listing_days"],
            label_horizon_days=training.label_horizon_days,
            rebalance_frequency=training_config.rebalance_frequency,
            rebalance_interval_trading_days=(
                training_config.rebalance_interval_trading_days
            ),
        )
        del raw_training_data
        if training_data_cache is not None:
            training_data_cache[cache_key] = training_data
    training_result = fit_factor_weights(
        training_data,
        factor_names,
        factor_parameters,
        train_period.start_date,
        train_period.end_date,
        minimum_history_days=resolved_parameters["min_listing_days"],
        winsorize_lower=resolved_parameters["winsorize_lower"],
        winsorize_upper=resolved_parameters["winsorize_upper"],
        label_horizon_days=training.label_horizon_days,
        ridge_alpha=training.ridge_alpha if ridge_alpha is None else ridge_alpha,
        max_iterations=training.max_iterations,
        minimum_training_observations=training.minimum_training_observations,
        minimum_training_dates=training.minimum_training_dates,
        prepared_data=training_data,
        prior_factor_weights=resolved_parameters["factor_weights"],
        rebalance_frequency=training_config.rebalance_frequency,
        rebalance_interval_trading_days=(
            training_config.rebalance_interval_trading_days
        ),
    )
    trained_parameters = dict(candidate_config.strategy_parameters)
    trained_weights = strategy.apply_trained_factor_weights(
        resolved_parameters,
        training_result.factor_weights,
    )
    if not trained_weights:
        raise ValueError("训练得到的因子权重全部退化为零")
    trained_parameters["factor_weights"] = trained_weights
    trained_parameters["factor_parameters"] = {
        name: dict(factor_parameters.get(name, {})) for name in trained_weights
    }
    trained_config = replace(
        candidate_config,
        strategy_parameters=trained_parameters,
        strategy_version=strategy.metadata.version,
    )
    return trained_config, training_result


def fit_factor_candidate_config(
    candidate_config: BacktestConfig,
    train_period: EvaluationPeriod,
    training: TrainingSpec,
    database_manager: DBManager,
    *,
    ridge_alpha: float | None = None,
) -> tuple[BacktestConfig, FactorTrainingResult]:
    """使用研究训练口径拟合一个候选，供最终生产重训复用。"""

    return _fit_candidate_config(
        candidate_config,
        train_period,
        training,
        database_manager,
        ridge_alpha=ridge_alpha,
    )


def validate_factor_training_result(
    training_result: FactorTrainingResult,
    training: TrainingSpec,
    validity: ResearchValiditySpec,
) -> None:
    """对最终生产训练结果应用与历史研究一致的样本和模型门禁。"""

    _enforce_validity_thresholds(
        phase="training",
        observation_count=training_result.observation_count,
        signal_date_count=training_result.signal_date_count,
        minimum_observations=training.minimum_training_observations,
        minimum_signal_dates=validity.minimum_training_signal_dates,
    )
    _enforce_training_model_validity(training_result, validity)


def build_research_reproducibility_audit(
    config: FactorExperimentEvaluationConfig,
    data_snapshot: dict[str, object] | None = None,
) -> dict[str, object]:
    """构建研究与生产报告共享的代码、配置、因子和资源审计。"""

    return _build_reproducibility_audit(config, data_snapshot)


def _summarize_target_coverage(
    targets: pd.DataFrame,
    period: EvaluationPeriod,
    daily_nav: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Summarize executable signal targets and reject dates outside the phase."""

    if not isinstance(targets, pd.DataFrame):
        raise ValueError("策略目标必须是 pandas.DataFrame")
    if targets.empty:
        return {
            "target_observation_count": 0,
            "signal_date_count": 0,
            "signal_date_start": None,
            "signal_date_end": None,
        }
    if "date" not in targets.columns:
        raise ValueError("策略目标缺少 date 字段，无法检查信号覆盖")
    parsed_dates = pd.to_datetime(targets["date"], errors="coerce")
    if parsed_dates.isna().any():
        raise ValueError("策略目标包含无法解析的信号日期")
    phase_dates = parsed_dates.dt.date
    outside_period = (phase_dates < period.start_date) | (phase_dates > period.end_date)
    if outside_period.any():
        invalid_dates = sorted(
            {value.isoformat() for value in phase_dates[outside_period]}
        )
        raise ValueError(
            f"策略目标信号日期超出 {period.start_date.isoformat()} ~ "
            f"{period.end_date.isoformat()}: {', '.join(invalid_dates[:3])}"
        )
    unique_dates = parsed_dates.dt.normalize().drop_duplicates().sort_values()
    if daily_nav is None:
        executable_dates = unique_dates
    else:
        if not isinstance(daily_nav, pd.DataFrame) or "date" not in daily_nav.columns:
            raise ValueError("每日净值缺少 date 字段，无法检查 T+1 执行覆盖")
        trading_dates = pd.to_datetime(daily_nav["date"], errors="coerce")
        if trading_dates.isna().any():
            raise ValueError("每日净值包含无法解析的交易日期")
        trading_calendar = trading_dates.dt.normalize()
        trading_calendar = trading_calendar[
            (trading_calendar.dt.date >= period.start_date)
            & (trading_calendar.dt.date <= period.end_date)
        ].drop_duplicates()
        executable_dates = pd.Series(
            [
                signal_date
                for signal_date in unique_dates
                if (trading_calendar > signal_date).any()
            ],
            dtype="datetime64[ns]",
        )
    executable_date_set = set(executable_dates)
    executable_rows = parsed_dates.dt.normalize().isin(executable_date_set)
    executable_dates = executable_dates.sort_values()
    if executable_dates.empty:
        return {
            "target_observation_count": 0,
            "signal_date_count": 0,
            "signal_date_start": None,
            "signal_date_end": None,
        }
    return {
        "target_observation_count": int(executable_rows.sum()),
        "signal_date_count": int(len(executable_dates)),
        "signal_date_start": executable_dates.iloc[0].date().isoformat(),
        "signal_date_end": executable_dates.iloc[-1].date().isoformat(),
    }


def _build_training_validity_row(
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    training_result: FactorTrainingResult | None,
    *,
    status: str,
    error: str,
    training: TrainingSpec | None,
    validity: ResearchValiditySpec,
) -> dict:
    weights = training_result.factor_weights if training_result is not None else {}
    squared_weight_sum = sum(float(weight) ** 2 for weight in weights.values())
    effective_factor_count = (
        1 / squared_weight_sum if squared_weight_sum > 1e-12 else None
    )
    return {
        "split_id": split.split_id,
        "candidate_id": trial.candidate_id,
        "trial_id": trial.trial_id,
        "phase": "training",
        "status": status,
        "error": error,
        "observation_count": (
            training_result.observation_count if training_result is not None else None
        ),
        "signal_date_count": (
            training_result.signal_date_count if training_result is not None else None
        ),
        "signal_date_start": (
            training_result.signal_date_start if training_result is not None else None
        ),
        "signal_date_end": (
            training_result.signal_date_end if training_result is not None else None
        ),
        "minimum_observations": (
            training.minimum_training_observations if training is not None else None
        ),
        "minimum_signal_dates": validity.minimum_training_signal_dates,
        "effective_factor_count": effective_factor_count,
        "minimum_effective_factor_count": validity.minimum_effective_factor_count,
        "maximum_factor_weight_actual": max(weights.values()) if weights else None,
        "maximum_factor_weight_allowed": validity.maximum_factor_weight,
        "selection_score": None,
        "minimum_selection_score": None,
    }


def _build_backtest_validity_row(
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    phase: str,
    coverage: dict[str, object],
    validity: ResearchValiditySpec,
    *,
    status: str,
    error: str,
) -> dict:
    minimum_observations, minimum_signal_dates = _get_validity_thresholds(
        validity, phase
    )
    return {
        "split_id": split.split_id,
        "candidate_id": trial.candidate_id,
        "trial_id": trial.trial_id,
        "phase": phase,
        "status": status,
        "error": error,
        "observation_count": coverage.get("target_observation_count"),
        "signal_date_count": coverage.get("signal_date_count"),
        "signal_date_start": coverage.get("signal_date_start"),
        "signal_date_end": coverage.get("signal_date_end"),
        "minimum_observations": minimum_observations,
        "minimum_signal_dates": minimum_signal_dates,
        "effective_factor_count": None,
        "minimum_effective_factor_count": None,
        "maximum_factor_weight_actual": None,
        "maximum_factor_weight_allowed": None,
        "selection_score": None,
        "minimum_selection_score": None,
    }


def _append_training_validity_check(
    validity_rows: list[dict],
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    training_result: FactorTrainingResult,
    validity: ResearchValiditySpec | None,
    training: TrainingSpec | None,
) -> None:
    if validity is None or not validity.enabled:
        return
    row = _build_training_validity_row(
        split,
        trial,
        training_result,
        status="passed",
        error="",
        training=training,
        validity=validity,
    )
    validity_rows.append(row)
    try:
        _enforce_validity_thresholds(
            phase="training",
            observation_count=training_result.observation_count,
            signal_date_count=training_result.signal_date_count,
            minimum_observations=(
                training.minimum_training_observations if training is not None else 0
            ),
            minimum_signal_dates=validity.minimum_training_signal_dates,
        )
        _enforce_training_model_validity(training_result, validity)
    except Exception as error:
        row["status"] = "failed"
        row["error"] = str(error)
        raise


def _append_backtest_validity_check(
    validity_rows: list[dict],
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    phase: str,
    coverage: dict[str, object],
    validity: ResearchValiditySpec | None,
) -> None:
    if validity is None or not validity.enabled:
        return
    row = _build_backtest_validity_row(
        split,
        trial,
        phase,
        coverage,
        validity,
        status="passed",
        error="",
    )
    validity_rows.append(row)
    minimum_observations, minimum_signal_dates = _get_validity_thresholds(
        validity, phase
    )
    try:
        _enforce_validity_thresholds(
            phase=phase,
            observation_count=int(coverage["target_observation_count"]),
            signal_date_count=int(coverage["signal_date_count"]),
            minimum_observations=minimum_observations,
            minimum_signal_dates=minimum_signal_dates,
        )
    except Exception as error:
        row["status"] = "failed"
        row["error"] = str(error)
        raise


def _append_validation_selection_score_check(
    validity_rows: list[dict],
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    selection_score: float,
    validity: ResearchValiditySpec | None,
) -> str:
    if (
        validity is None
        or not validity.enabled
        or validity.minimum_validation_selection_score is None
    ):
        return ""
    minimum_score = validity.minimum_validation_selection_score
    status = "passed"
    error = ""
    if selection_score + SELECTION_SCORE_TOLERANCE < minimum_score:
        status = "rejected"
        error = (
            "研究有效性门禁拒绝 [validation_selection_score]: "
            f"{selection_score:.6f} < minimum_validation_selection_score="
            f"{minimum_score}"
        )
    validity_rows.append(
        {
            "split_id": split.split_id,
            "candidate_id": trial.candidate_id,
            "trial_id": trial.trial_id,
            "phase": "validation_selection",
            "status": status,
            "error": error,
            "observation_count": None,
            "signal_date_count": None,
            "signal_date_start": None,
            "signal_date_end": None,
            "minimum_observations": None,
            "minimum_signal_dates": None,
            "effective_factor_count": None,
            "minimum_effective_factor_count": None,
            "maximum_factor_weight_actual": None,
            "maximum_factor_weight_allowed": None,
            "selection_score": selection_score,
            "minimum_selection_score": minimum_score,
        }
    )
    return error


def _build_protocol_row(
    *,
    split_id: str,
    gate: str,
    status: str,
    actual_value: object,
    required_value: object,
    error: str = "",
) -> dict:
    return {
        "split_id": split_id,
        "gate": gate,
        "status": status,
        "error": error,
        "actual_value": actual_value,
        "required_value": required_value,
    }


def _append_split_protocol_checks(
    protocol_rows: list[dict],
    split: EvaluationSplit,
    *,
    trial_count: int,
    training_fit_failure_count: int,
    validation_coverage: dict[str, dict[str, object]],
    validity: ResearchValiditySpec | None,
) -> str:
    if validity is None or not validity.enabled:
        return ""
    errors = []
    failure_ratio = training_fit_failure_count / trial_count if trial_count else 1.0
    failure_status = (
        "passed"
        if failure_ratio
        <= validity.maximum_training_failure_ratio + SELECTION_SCORE_TOLERANCE
        else "failed"
    )
    failure_error = ""
    if failure_status == "failed":
        failure_error = (
            "研究协议门禁失败 [training_failure_ratio]: "
            f"{failure_ratio:.6f} > maximum_training_failure_ratio="
            f"{validity.maximum_training_failure_ratio}"
        )
        errors.append(failure_error)
    protocol_rows.append(
        _build_protocol_row(
            split_id=split.split_id,
            gate="training_failure_ratio",
            status=failure_status,
            error=failure_error,
            actual_value=failure_ratio,
            required_value=validity.maximum_training_failure_ratio,
        )
    )

    maximum_trial_ratio = validity.maximum_trials_per_validation_signal_date
    if maximum_trial_ratio is not None:
        signal_date_counts = [
            int(coverage.get("signal_date_count") or 0)
            for coverage in validation_coverage.values()
        ]
        validation_signal_date_count = (
            min(signal_date_counts) if signal_date_counts else 0
        )
        evaluated_trial_count = len(validation_coverage)
        trial_ratio = (
            evaluated_trial_count / validation_signal_date_count
            if validation_signal_date_count
            else math.inf
        )
        ratio_status = (
            "passed"
            if trial_ratio <= maximum_trial_ratio + SELECTION_SCORE_TOLERANCE
            else "failed"
        )
        ratio_error = ""
        if ratio_status == "failed":
            ratio_error = (
                "研究协议门禁失败 [trials_per_validation_signal_date]: "
                f"{trial_ratio:.6f} > "
                "maximum_trials_per_validation_signal_date="
                f"{maximum_trial_ratio}"
            )
            errors.append(ratio_error)
        protocol_rows.append(
            _build_protocol_row(
                split_id=split.split_id,
                gate="trials_per_validation_signal_date",
                status=ratio_status,
                error=ratio_error,
                actual_value=trial_ratio,
                required_value=maximum_trial_ratio,
            )
        )
    return "; ".join(errors)


def _get_validity_thresholds(
    validity: ResearchValiditySpec, phase: str
) -> tuple[int, int]:
    if phase == "validation":
        return (
            validity.minimum_validation_observations,
            validity.minimum_validation_signal_dates,
        )
    if phase == "test":
        return (
            validity.minimum_test_observations,
            validity.minimum_test_signal_dates,
        )
    raise ValueError(f"不支持的研究有效性阶段: {phase}")


def _enforce_training_model_validity(
    training_result: FactorTrainingResult,
    validity: ResearchValiditySpec,
) -> None:
    weights = [float(weight) for weight in training_result.factor_weights.values()]
    squared_weight_sum = sum(weight**2 for weight in weights)
    effective_factor_count = (
        1 / squared_weight_sum if squared_weight_sum > 1e-12 else 0.0
    )
    maximum_factor_weight = max(weights, default=0.0)
    failures = []
    if effective_factor_count + SELECTION_SCORE_TOLERANCE < (
        validity.minimum_effective_factor_count
    ):
        failures.append(
            f"有效因子数 {effective_factor_count:.6f} < "
            "minimum_effective_factor_count="
            f"{validity.minimum_effective_factor_count}"
        )
    if maximum_factor_weight - SELECTION_SCORE_TOLERANCE > (
        validity.maximum_factor_weight
    ):
        failures.append(
            f"最大因子权重 {maximum_factor_weight:.6f} > "
            f"maximum_factor_weight={validity.maximum_factor_weight}"
        )
    if failures:
        raise ResearchValidityError(
            "研究有效性门禁失败 [training_model]: " + "; ".join(failures)
        )


def _enforce_validity_thresholds(
    *,
    phase: str,
    observation_count: int,
    signal_date_count: int,
    minimum_observations: int,
    minimum_signal_dates: int,
) -> None:
    failures = []
    if observation_count < minimum_observations:
        failures.append(
            f"观测数 {observation_count} < minimum_observations={minimum_observations}"
        )
    if signal_date_count < minimum_signal_dates:
        failures.append(
            f"信号日 {signal_date_count} < minimum_signal_dates={minimum_signal_dates}"
        )
    if failures:
        raise ResearchValidityError(
            f"研究有效性门禁失败 [{phase}]: " + "; ".join(failures)
        )


def _build_training_row(
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    training_result: FactorTrainingResult,
) -> dict:
    details = training_result.to_dict()
    parameter_fields = _build_trial_parameter_fields(trial.parameters)
    return {
        "split_id": split.split_id,
        "candidate_id": trial.candidate_id,
        "trial_id": trial.trial_id,
        "train_start_date": split.train.start_date.isoformat(),
        "train_end_date": split.train.end_date.isoformat(),
        "status": "fitted",
        "error": "",
        **parameter_fields,
        "factor_weights": json.dumps(
            details.pop("factor_weights"), ensure_ascii=False, sort_keys=True
        ),
        "prior_factor_weights": json.dumps(
            details.pop("prior_factor_weights"),
            ensure_ascii=False,
            sort_keys=True,
        ),
        **details,
    }


def _build_failed_training_row(
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    error: Exception,
    *,
    training_result: FactorTrainingResult | None = None,
) -> dict:
    parameter_fields = _build_trial_parameter_fields(trial.parameters)
    details = training_result.to_dict() if training_result is not None else {}
    return {
        "split_id": split.split_id,
        "candidate_id": trial.candidate_id,
        "trial_id": trial.trial_id,
        "train_start_date": split.train.start_date.isoformat(),
        "train_end_date": split.train.end_date.isoformat(),
        "status": "failed",
        "error": str(error),
        **parameter_fields,
        "factor_weights": json.dumps(
            details.get("factor_weights", {}),
            ensure_ascii=False,
            sort_keys=True,
        )
        if training_result is not None
        else "",
        "observation_count": details.get("observation_count"),
        "signal_date_count": details.get("signal_date_count"),
        "signal_date_start": details.get("signal_date_start"),
        "signal_date_end": details.get("signal_date_end"),
        "iterations": details.get("iterations"),
        "converged": details.get("converged"),
        "label_horizon_days": details.get("label_horizon_days"),
        "target_transform": details.get("target_transform"),
        "sample_weighting": details.get("sample_weighting"),
        "weight_constraint": details.get("weight_constraint"),
        "rebalance_frequency": details.get(
            "rebalance_frequency", trial.config.rebalance_frequency
        ),
        "rebalance_interval_trading_days": details.get(
            "rebalance_interval_trading_days",
            trial.config.rebalance_interval_trading_days,
        ),
        "rebalance_anchor_date": details.get(
            "rebalance_anchor_date",
            _replace_backtest_period(trial.config, split.train).rebalance_anchor_date,
        ),
        "prior_factor_weights": json.dumps(
            details.get("prior_factor_weights"),
            ensure_ascii=False,
            sort_keys=True,
        )
        if training_result is not None
        else "",
    }


def _build_metric_row(
    split: EvaluationSplit,
    phase_name: str,
    candidate_id: str,
    config: BacktestConfig,
    period: EvaluationPeriod,
    metrics: dict,
    *,
    coverage: dict | None = None,
    trial_id: str | None = None,
) -> dict:
    row = {
        "split_id": split.split_id,
        "phase": phase_name,
        "candidate_id": candidate_id,
        "strategy_name": config.strategy_name,
        "start_date": period.start_date.isoformat(),
        "end_date": period.end_date.isoformat(),
        **metrics,
        **(coverage or {}),
    }
    if trial_id is not None:
        row["trial_id"] = trial_id
    return row


def _build_trial_parameter_fields(parameters: dict[str, object]) -> dict:
    if not parameters:
        return {
            "holding_count": None,
            "winsorize_lower": None,
            "winsorize_upper": None,
            "ridge_alpha": None,
            "portfolio_weighting": None,
            "factor_parameters": "",
        }
    return {
        "holding_count": parameters["holding_count"],
        "winsorize_lower": parameters["winsorize_lower"],
        "winsorize_upper": parameters["winsorize_upper"],
        "ridge_alpha": parameters["ridge_alpha"],
        "portfolio_weighting": parameters.get("portfolio_weighting", "equal"),
        "factor_parameters": json.dumps(
            parameters["factor_parameters"], ensure_ascii=False, sort_keys=True
        ),
    }


def _build_hyperparameter_trial_row(
    split: EvaluationSplit,
    trial: HyperparameterTrial,
    *,
    status: str,
    error: str = "",
    train_metrics: dict | None = None,
    validation_metrics: dict | None = None,
    factor_weights: dict[str, float] | None = None,
) -> dict:
    train_metrics = train_metrics or {}
    validation_metrics = validation_metrics or {}
    row = {
        "split_id": split.split_id,
        "candidate_id": trial.candidate_id,
        "trial_id": trial.trial_id,
        "status": status,
        "error": error,
        **_build_trial_parameter_fields(trial.parameters),
        "factor_weights": (
            json.dumps(factor_weights, ensure_ascii=False, sort_keys=True)
            if factor_weights
            else ""
        ),
    }
    for prefix, metrics in (
        ("train", train_metrics),
        ("validation", validation_metrics),
    ):
        for metric_name in (
            "total_return",
            "annualized_return",
            "annualized_volatility",
            "sharpe_ratio",
            "max_drawdown",
            "trading_days",
        ):
            row[f"{prefix}_{metric_name}"] = metrics.get(metric_name)
        row[f"{prefix}_target_observation_count"] = metrics.get(
            "target_observation_count"
        )
        row[f"{prefix}_signal_date_count"] = metrics.get("signal_date_count")
    return row


def _select_candidate(
    scores: dict[str, float | None], direction: str, split_id: str
) -> tuple[str, float]:
    valid_scores = [
        (candidate_id, score)
        for candidate_id, score in scores.items()
        if score is not None and math.isfinite(float(score))
    ]
    if not valid_scores:
        raise ValueError(f"{split_id} 的所有候选验证指标均为空，无法选择方案")
    selected_candidate, selected_score = valid_scores[0]
    for candidate_id, score in valid_scores[1:]:
        if (direction == "max" and score > selected_score) or (
            direction == "min" and score < selected_score
        ):
            selected_candidate, selected_score = candidate_id, score
    return selected_candidate, float(selected_score)


def _build_reproducibility_audit(
    config: FactorExperimentEvaluationConfig,
    data_snapshot: dict[str, object] | None = None,
) -> dict[str, object]:
    candidate_hashes = {}
    factor_versions = {}
    rebalance_schedules = {}
    search_parameter_coverage = {}
    configured_factor_parameters = (
        config.hyperparameter_search.factor_parameter_values
        if config.hyperparameter_search is not None
        and config.hyperparameter_search.enabled
        else {}
    )
    for path in config.candidate_configs:
        try:
            candidate_hashes[str(path)] = _sha256_file(path)
        except OSError as error:
            candidate_hashes[str(path)] = f"未取得: {error}"
        try:
            candidate_config = load_backtest_config(path)
            strategy = get_backtest_strategy(candidate_config.strategy_name)
            resolved = strategy.validate_parameters(
                candidate_config.strategy_parameters
            )
            factor_versions[path.stem] = resolved.get("factor_versions", {})
            rebalance_schedules[path.stem] = {
                "rebalance_frequency": candidate_config.rebalance_frequency,
                "rebalance_interval_trading_days": (
                    candidate_config.rebalance_interval_trading_days
                ),
                "anchor": (
                    "evaluation_split_start_date"
                    if candidate_config.rebalance_frequency == "every_n_trading_days"
                    else "fixed_calendar_period"
                ),
            }
            selected_factors = set(factor_versions[path.stem])
            configured_factors = set(configured_factor_parameters)
            search_parameter_coverage[path.stem] = {
                "applied_factor_parameters": sorted(
                    configured_factors & selected_factors
                ),
                "ignored_factor_parameters": sorted(
                    configured_factors - selected_factors
                ),
            }
        except Exception as error:
            factor_versions[path.stem] = {"_error": str(error)}
            rebalance_schedules[path.stem] = {"_error": str(error)}
            search_parameter_coverage[path.stem] = {"_error": str(error)}

    git_commit = "未取得"
    git_worktree_dirty = None
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path.cwd(),
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
        if completed.returncode == 0 and completed.stdout.strip():
            git_commit = completed.stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=Path.cwd(),
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
        if status.returncode == 0:
            git_worktree_dirty = bool(status.stdout.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    resolved_data_snapshot = data_snapshot or {
        "snapshot_id": "unavailable",
        "status": "unavailable",
        "reason": "本次结果未采集数据快照",
    }
    peak_rss_bytes = _get_process_peak_rss_bytes()
    return {
        "candidate_config_sha256": candidate_hashes,
        "factor_versions": factor_versions,
        "rebalance_schedules": rebalance_schedules,
        "search_parameter_coverage": search_parameter_coverage,
        "git_commit": git_commit,
        "git_worktree_dirty": git_worktree_dirty,
        "data_version": resolved_data_snapshot.get("snapshot_id", "unavailable"),
        "data_snapshot": resolved_data_snapshot,
        "resource": {
            "peak_rss_bytes": peak_rss_bytes,
            "peak_rss_limit_bytes": RESEARCH_PEAK_RSS_LIMIT_BYTES,
            "peak_rss_exceeded": (
                peak_rss_bytes > RESEARCH_PEAK_RSS_LIMIT_BYTES
                if peak_rss_bytes is not None
                else None
            ),
            "training_cache_max_entries": (
                config.training.max_training_cache_entries
                if config.training is not None and config.training.enabled
                else None
            ),
        },
    }


def _get_process_peak_rss_bytes() -> int | None:
    """Return this evaluation process' peak RSS in bytes when supported."""

    if resource is None:
        return None
    try:
        peak_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (AttributeError, OSError, ValueError):
        return None
    if sys.platform == "darwin":
        return peak_rss
    return peak_rss * 1024


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_evaluation_summary(
    path: Path,
    result: FactorExperimentEvaluationResult,
    audit: dict[str, object] | None = None,
) -> None:
    path.write_text(
        _build_evaluation_summary(
            result, audit or _build_reproducibility_audit(result.config)
        ),
        encoding="utf-8",
    )


def _build_evaluation_summary(
    result: FactorExperimentEvaluationResult,
    audit: dict[str, object] | None = None,
) -> str:
    config = result.config
    audit = audit or _build_reproducibility_audit(config)
    training_enabled = config.training is not None and config.training.enabled
    search_enabled = (
        config.hyperparameter_search is not None
        and config.hyperparameter_search.enabled
    )
    training_rows = list(result.training_rows)
    trial_rows = list(result.hyperparameter_rows if search_enabled else training_rows)
    failed_training_rows = [
        row for row in training_rows if row.get("status") == "failed"
    ]
    rejected_training_rows = [
        row for row in training_rows if row.get("status") == "rejected"
    ]
    report_status = _get_report_status(result, len(failed_training_rows))
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    lines = [
        "# 因子训练标准结果报告",
        "",
        "本报告是研究评估结果的标准人读入口；完整参数、组合明细和阶段指标分别保存在同目录 CSV 文件中。",
        "",
        "## 1. 报告身份与执行状态",
        "",
    ]
    _append_markdown_table(
        lines,
        ["项目", "内容"],
        [
            ("报告版本", REPORT_SCHEMA_VERSION),
            ("研究名称", config.name),
            ("生成时间", generated_at),
            ("执行状态", report_status),
            ("研究协议状态", result.protocol_status),
            ("策略证据状态", result.strategy_evidence_status),
            (
                "回溯性方法开发",
                str(config.retrospective_method_development).lower(),
            ),
            (
                "选择指标",
                f"`{config.selection_metric}`（{config.selection_direction}）",
            ),
            (
                "训练模式",
                "外层有限网格 + 每组独立拟合"
                if search_enabled
                else "每个候选独立拟合"
                if training_enabled
                else "未启用权重训练",
            ),
            ("代码版本", audit.get("git_commit", "未取得")),
            (
                "代码工作区",
                "dirty"
                if audit.get("git_worktree_dirty") is True
                else "clean"
                if audit.get("git_worktree_dirty") is False
                else "未取得",
            ),
            ("数据版本", audit.get("data_version", "未取得")),
            (
                "数据快照详情",
                _format_json_value(audit.get("data_snapshot", {})),
            ),
            (
                "资源审计",
                _format_json_value(audit.get("resource", {})),
            ),
        ],
    )

    lines.extend(["", "## 2. 研究配置与时间边界", ""])
    candidate_ids = [path.stem for path in config.candidate_configs]
    _append_markdown_table(
        lines,
        ["项目", "内容"],
        [
            ("候选数量", len(candidate_ids)),
            (
                "候选 ID",
                ", ".join(f"`{candidate_id}`" for candidate_id in candidate_ids),
            ),
            (
                "候选配置路径",
                "<br>".join(str(path) for path in config.candidate_configs),
            ),
            (
                "候选配置 SHA256",
                "<br>".join(
                    f"{path}: {digest}"
                    for path, digest in audit.get("candidate_config_sha256", {}).items()
                ),
            ),
            (
                "因子版本",
                _format_json_value(audit.get("factor_versions", {})),
            ),
            (
                "搜索参数覆盖",
                _format_json_value(audit.get("search_parameter_coverage", {})),
            ),
            (
                "候选调仓日程",
                _format_json_value(audit.get("rebalance_schedules", {})),
            ),
            ("信号执行", "信号日收盘后生成，下一交易日开盘成交"),
            (
                "标签定义",
                f"信号日后下一交易日开盘至 {config.training.label_horizon_days if training_enabled else '固定'} 个交易观察后的收盘收益；训练时按信号日转为截面百分位排名并去均值"
                if training_enabled
                else "未启用训练标签",
            ),
            (
                "训练目标权重",
                "每个信号日总权重相等；非负权重和为 1，并向候选策略先验权重收缩"
                if training_enabled
                else "未启用训练",
            ),
            (
                "固定切分",
                _format_split(config.fixed_split),
            ),
            (
                "Walk-forward",
                _format_walk_forward(config.walk_forward),
            ),
            (
                "固定训练参数",
                _format_training_parameters(config.training),
            ),
            (
                "研究有效性门槛",
                _format_validity_parameters(config.validity),
            ),
            (
                "外层搜索空间",
                _format_search_parameters(config.hyperparameter_search),
            ),
        ],
    )
    lines.extend(["", "### 各评估窗口", ""])
    _append_markdown_table(
        lines,
        ["窗口", "训练区间", "验证区间", "测试区间"],
        [
            (
                split.split_id,
                _format_period(split.train),
                _format_period(split.validation),
                _format_period(split.test),
            )
            for split in config.get_splits()
        ],
    )

    lines.extend(["", "## 3. 点时数据与样本覆盖", ""])
    if not training_enabled:
        lines.append("本次未启用因子权重训练，因此没有训练信号日和训练样本统计。")
    else:
        lines.append(
            "训练样本按候选配置的调仓信号日构造；股票观测行数是横截面样本，不能代替时间维度的信号日数量。"
        )
        lines.append("")
        coverage_rows = []
        for split_id in _ordered_split_ids(config):
            split_rows = [
                row
                for row in training_rows
                if row.get("split_id") == split_id and row.get("status") == "fitted"
            ]
            if not split_rows:
                coverage_rows.append((split_id, "—", "—", "—", "—", "—"))
                continue
            signal_counts = [
                int(value)
                for value in (
                    _coerce_finite_number(row.get("signal_date_count"))
                    for row in split_rows
                )
                if value is not None
            ]
            observation_counts = [
                int(value)
                for value in (
                    _coerce_finite_number(row.get("observation_count"))
                    for row in split_rows
                )
                if value is not None
            ]
            signal_starts = [
                row.get("signal_date_start")
                for row in split_rows
                if row.get("signal_date_start")
            ]
            signal_ends = [
                row.get("signal_date_end")
                for row in split_rows
                if row.get("signal_date_end")
            ]
            coverage_rows.append(
                (
                    split_id,
                    _format_range(signal_counts),
                    _format_range(observation_counts),
                    min(signal_starts) if signal_starts else "—",
                    max(signal_ends) if signal_ends else "—",
                    len(split_rows),
                )
            )
        _append_markdown_table(
            lines,
            [
                "窗口",
                "信号日数（最小~最大）",
                "观测数（最小~最大）",
                "最早信号日",
                "最晚信号日",
                "成功组合数",
            ],
            coverage_rows,
        )
        lines.extend(
            [
                "",
                "训练结束日之后可能额外读取标签所需行情；这些行情只用于未来收益标签，不进入训练回测收益统计。",
            ]
        )

    lines.extend(["", "### 研究有效性门禁", ""])
    if config.validity is None or not config.validity.enabled:
        lines.append("本次未启用研究有效性门禁。")
    elif not result.validity_rows:
        lines.append("研究有效性门禁已配置，但本次没有产生可检查的阶段记录。")
    else:
        lines.append(
            "门禁检查样本覆盖、权重结构和最低验证证据；通过不等于策略收益有效。完整逐组合记录见 `research_validity.csv`。"
        )
        lines.append("")
        validity_summary_rows = []
        for split_id in _ordered_split_ids(config):
            for phase in (
                "training",
                "validation",
                "validation_selection",
                "test",
            ):
                phase_rows = [
                    row
                    for row in result.validity_rows
                    if row.get("split_id") == split_id and row.get("phase") == phase
                ]
                if not phase_rows:
                    continue
                signal_counts = [
                    int(value)
                    for value in (
                        _coerce_finite_number(row.get("signal_date_count"))
                        for row in phase_rows
                    )
                    if value is not None
                ]
                observation_counts = [
                    int(value)
                    for value in (
                        _coerce_finite_number(row.get("observation_count"))
                        for row in phase_rows
                    )
                    if value is not None
                ]
                minimum_signal_dates = next(
                    (
                        row.get("minimum_signal_dates")
                        for row in phase_rows
                        if row.get("minimum_signal_dates") is not None
                    ),
                    "—",
                )
                minimum_observations = next(
                    (
                        row.get("minimum_observations")
                        for row in phase_rows
                        if row.get("minimum_observations") is not None
                    ),
                    "—",
                )
                validity_summary_rows.append(
                    (
                        split_id,
                        phase,
                        len(phase_rows),
                        sum(row.get("status") == "passed" for row in phase_rows),
                        sum(row.get("status") != "passed" for row in phase_rows),
                        _format_range(signal_counts),
                        minimum_signal_dates,
                        _format_range(observation_counts),
                        minimum_observations,
                    )
                )
        _append_markdown_table(
            lines,
            [
                "窗口",
                "阶段",
                "检查数",
                "通过",
                "失败",
                "信号日（最小~最大）",
                "信号日门槛",
                "观测数（最小~最大）",
                "观测数门槛",
            ],
            validity_summary_rows,
        )
        failed_validity_rows = [
            row for row in result.validity_rows if row.get("status") != "passed"
        ]
        if failed_validity_rows:
            lines.extend(["", "门禁失败示例：", ""])
            _append_markdown_table(
                lines,
                ["窗口", "候选", "试验", "阶段", "原因"],
                [
                    (
                        row.get("split_id"),
                        row.get("candidate_id"),
                        row.get("trial_id"),
                        row.get("phase"),
                        row.get("error"),
                    )
                    for row in failed_validity_rows[:20]
                ],
            )
            if len(failed_validity_rows) > 20:
                lines.append("仅展示前 20 条，完整失败记录见 `research_validity.csv`。")

    lines.extend(["", "### 研究协议门禁", ""])
    if not result.protocol_rows:
        lines.append("本次没有启用研究协议门禁。")
    else:
        lines.append(
            "协议状态只表示预先声明的搜索与窗口规则是否被遵守；策略收益证据在测试段单独陈述。"
        )
        lines.append("")
        _append_markdown_table(
            lines,
            ["窗口", "门禁", "状态", "实际值", "要求值", "原因"],
            [
                (
                    row.get("split_id"),
                    row.get("gate"),
                    row.get("status"),
                    _format_report_value(row.get("actual_value")),
                    _format_report_value(row.get("required_value")),
                    row.get("error") or "—",
                )
                for row in result.protocol_rows
            ],
        )

    lines.extend(["", "## 4. 搜索与训练状态", ""])
    if not training_enabled:
        lines.append("本次未启用训练，不产生拟合组合或训练失败记录。")
    else:
        fitted_count = sum(row.get("status") == "fitted" for row in training_rows)
        failed_count = len(failed_training_rows)
        success_rate = fitted_count / len(training_rows) if training_rows else None
        validation_rejected_count = sum(
            row.get("status") == "rejected" for row in result.hyperparameter_rows
        )
        _append_markdown_table(
            lines,
            ["指标", "数值"],
            [
                ("候选因子组合数量", len(candidate_ids)),
                (
                    "展开参数组合数量",
                    len(trial_rows) if search_enabled else "未启用外层搜索",
                ),
                ("成功拟合数量", fitted_count),
                ("模型门禁拒绝数量", len(rejected_training_rows)),
                ("训练失败数量", failed_count),
                ("训练成功率", _format_percent(success_rate)),
                ("验证门禁拒绝数量", validation_rejected_count),
                (
                    "失败原因种类数",
                    len(
                        Counter(
                            row.get("error") or "未知原因"
                            for row in failed_training_rows
                        )
                    ),
                ),
            ],
        )
        lines.extend(["", "### 按窗口的训练状态", ""])
        status_rows = []
        for split_id in _ordered_split_ids(config):
            split_rows = [
                row for row in training_rows if row.get("split_id") == split_id
            ]
            status_rows.append(
                (
                    split_id,
                    len(split_rows),
                    sum(row.get("status") == "fitted" for row in split_rows),
                    sum(row.get("status") == "rejected" for row in split_rows),
                    sum(row.get("status") == "failed" for row in split_rows),
                )
            )
        _append_markdown_table(
            lines,
            ["窗口", "组合总数", "成功拟合", "模型拒绝", "训练失败"],
            status_rows,
        )
        lines.extend(["", "### 失败原因", ""])
        failure_counts = Counter(
            row.get("error") or "未知原因" for row in failed_training_rows
        )
        if failure_counts:
            _append_markdown_table(
                lines,
                ["失败原因", "次数"],
                [(reason, count) for reason, count in failure_counts.most_common()],
            )
        else:
            lines.append("无训练失败组合。")

        if search_enabled:
            lines.extend(["", "### 验证集 Top 10 组合", ""])
            _append_top_trials(lines, result)
        if result.evaluation_failure_rows:
            lines.extend(["", "### 回测或指标失败", ""])
            _append_markdown_table(
                lines,
                ["窗口", "候选", "试验", "阶段", "原因"],
                [
                    (
                        row.get("split_id"),
                        row.get("candidate_id"),
                        row.get("trial_id"),
                        row.get("phase"),
                        row.get("error"),
                    )
                    for row in result.evaluation_failure_rows
                ],
            )

    lines.extend(["", "### 验证集选择稳健性", ""])
    diagnostic_rows = list(result.selection_diagnostic_rows)
    if not diagnostic_rows:
        lines.append("本次没有产生验证集选择诊断。")
    else:
        lines.append(
            "本节只量化验证集选择负担，不改变入选规则；完整明细见 `selection_diagnostics.csv`。"
        )
        lines.append("")
        _append_markdown_table(
            lines,
            [
                "窗口",
                "状态",
                "候选数",
                "比较组合数",
                "验证信号日",
                "验证观测数",
                "入选试验",
                "入选分数",
                "第二名分数",
                "第一/二名差距",
                "并列第一数",
                "组合/信号日",
                "风险级别",
            ],
            [
                (
                    row.get("split_id"),
                    row.get("status"),
                    row.get("candidate_count"),
                    row.get("fitted_trial_count"),
                    row.get("validation_signal_date_count"),
                    row.get("validation_observation_count"),
                    row.get("selected_trial_id"),
                    _format_metric(row.get("selected_validation_score")),
                    _format_metric(row.get("second_validation_score")),
                    _format_metric(row.get("selection_score_margin")),
                    row.get("tied_best_trial_count"),
                    _format_metric(row.get("trials_per_validation_signal_date")),
                    row.get("risk_level"),
                )
                for row in diagnostic_rows
            ],
        )

    lines.extend(["", "## 5. 入选模型与拟合权重", ""])
    training_lookup = {
        (row.get("split_id"), row.get("trial_id")): row for row in training_rows
    }
    selected_training_rows = []
    selected_model_rows = []
    for selection in result.selection_rows:
        if selection.get("status", "completed") != "completed":
            continue
        trial_id = selection.get("selected_trial_id") or selection.get(
            "selected_candidate"
        )
        training_row = training_lookup.get((selection.get("split_id"), trial_id), {})
        if training_row:
            selected_training_rows.append(training_row)
        weight_map = _parse_json_mapping(selection.get("selected_factor_weights"))
        if not weight_map:
            weight_map = _parse_json_mapping(training_row.get("factor_weights"))
        weight_values = list(weight_map.values())
        weight_sum = sum(weight_values) if weight_values else None
        max_weight = max(weight_values) if weight_values else None
        max_factor = max(weight_map, key=weight_map.get) if weight_map else "—"
        selected_model_rows.append(
            (
                selection.get("split_id"),
                selection.get("selected_candidate"),
                trial_id,
                selection.get("selected_factor_parameters", "—"),
                selection.get("selected_holding_count", "—"),
                selection.get("selected_portfolio_weighting", "equal"),
                _format_winsorize_range(selection),
                selection.get("selected_ridge_alpha", "—"),
                _format_weights(weight_map),
                _format_report_value(weight_sum),
                sum(value > 1e-12 for value in weight_values),
                max_factor,
                _format_metric(max_weight),
            )
        )
    if selected_model_rows:
        _append_markdown_table(
            lines,
            [
                "窗口",
                "候选",
                "试验",
                "因子参数",
                "持仓数",
                "仓位法",
                "缩尾范围",
                "Ridge",
                "拟合权重",
                "权重和",
                "非零因子数",
                "最大权重因子",
                "最大权重",
            ],
            selected_model_rows,
        )
    else:
        lines.append("没有可展示的入选模型。")

    weight_diagnostic_rows = _build_factor_weight_diagnostic_rows(result)
    fitted_weight_diagnostic_rows = [
        row for row in weight_diagnostic_rows if row.get("status") == "fitted"
    ]
    lines.extend(["", "### 全部训练组合的权重集中度", ""])
    if not training_enabled:
        lines.append("本次未启用权重训练，无法计算权重集中度。")
    elif fitted_weight_diagnostic_rows:
        weight_summary_rows = []
        for split_id in _ordered_split_ids(result.config):
            split_rows = [
                row
                for row in fitted_weight_diagnostic_rows
                if row.get("split_id") == split_id
            ]
            if not split_rows:
                continue
            single_factor_count = sum(
                row.get("collapse_level") == "single_factor" for row in split_rows
            )
            effective_factor_counts = [
                value
                for value in (
                    _coerce_finite_number(row.get("effective_factor_count"))
                    for row in split_rows
                )
                if value is not None
            ]
            max_weights = [
                value
                for value in (
                    _coerce_finite_number(row.get("max_weight")) for row in split_rows
                )
                if value is not None
            ]
            selected_rows = [row for row in split_rows if row.get("selected")]
            selected_level = (
                selected_rows[0].get("collapse_level") if selected_rows else "—"
            )
            max_weight_range = (
                f"{_format_metric(min(max_weights))}~{_format_metric(max(max_weights))}"
                if max_weights
                else "—"
            )
            weight_summary_rows.append(
                (
                    split_id,
                    len(split_rows),
                    single_factor_count,
                    _format_percent(single_factor_count / len(split_rows)),
                    selected_level,
                    _format_metric(_mean(effective_factor_counts)),
                    _format_metric(min(effective_factor_counts))
                    if effective_factor_counts
                    else "—",
                    max_weight_range,
                )
            )
        _append_markdown_table(
            lines,
            [
                "窗口",
                "成功训练组合",
                "单因子组合",
                "单因子比例",
                "入选塌缩级别",
                "平均有效因子数",
                "最小有效因子数",
                "最大权重范围",
            ],
            weight_summary_rows,
        )
        lines.append(
            "该表统计全部成功训练组合；失败组合及逐组合指标见 `factor_weight_diagnostics.csv`。"
        )
    else:
        lines.append("本次没有成功训练组合，无法计算权重集中度。")

    walk_forward_training_rows = [
        row
        for row in selected_training_rows
        if str(row.get("split_id", "")).startswith("walk_forward_")
    ]
    if len(walk_forward_training_rows) > 1:
        lines.extend(["", "### Walk-forward 权重稳定性", ""])
        factor_names = sorted(
            {
                factor_name
                for row in walk_forward_training_rows
                for factor_name in _parse_json_mapping(row.get("factor_weights"))
            }
        )
        stability_rows = []
        for factor_name in factor_names:
            values = [
                float(weights[factor_name])
                for row in walk_forward_training_rows
                if (weights := _parse_json_mapping(row.get("factor_weights"))).get(
                    factor_name
                )
                is not None
            ]
            stability_rows.append(
                (
                    factor_name,
                    _format_metric(_mean(values)),
                    _format_metric(_population_std(values)),
                    _format_metric(min(values)) if values else "—",
                    _format_metric(max(values)) if values else "—",
                    sum(value > 1e-12 for value in values),
                )
            )
        _append_markdown_table(
            lines,
            ["因子", "均值", "标准差", "最小值", "最大值", "非零窗口数"],
            stability_rows,
        )
    elif config.walk_forward is not None:
        lines.extend(
            [
                "",
                "当前 Walk-forward 只有一个有效入选窗口，无法计算跨窗口权重稳定性。",
            ]
        )
    elif selected_training_rows:
        lines.extend(["", "当前只有固定切分，无法计算 Walk-forward 权重稳定性。"])

    lines.extend(["", "## 6. 训练、验证和测试表现", ""])
    metric_rows = []
    for selection in result.selection_rows:
        if selection.get("status", "completed") != "completed":
            continue
        for phase in ("train", "validation", "test"):
            metrics = _find_selected_metric_row(result.metric_rows, selection, phase)
            metric_rows.append(
                (
                    selection.get("split_id"),
                    phase,
                    _format_metric(metrics.get("total_return")) if metrics else "—",
                    _format_metric(metrics.get("annualized_return"))
                    if metrics
                    else "—",
                    _format_metric(metrics.get("annualized_volatility"))
                    if metrics
                    else "—",
                    _format_metric(metrics.get("sharpe_ratio")) if metrics else "—",
                    _format_metric(metrics.get("max_drawdown")) if metrics else "—",
                    _format_report_value(metrics.get("trading_days"))
                    if metrics
                    else "—",
                    _format_report_value(metrics.get("target_observation_count"))
                    if metrics
                    else "—",
                    _format_report_value(metrics.get("signal_date_count"))
                    if metrics
                    else "—",
                )
            )
    _append_markdown_table(
        lines,
        [
            "窗口",
            "阶段",
            "总收益",
            "年化收益",
            "年化波动",
            "夏普",
            "最大回撤",
            "交易日数",
            "目标观测数",
            "信号日数",
        ],
        metric_rows,
    )
    lines.extend(
        [
            "",
            "训练集和验证集使用各自组合拟合出的冻结权重；测试集只运行验证集入选组合，验证指标参与选择，因此不等同于最终无偏表现。",
        ]
    )
    walk_forward_selections = [
        row
        for row in result.selection_rows
        if str(row.get("split_id", "")).startswith("walk_forward_")
        and row.get("status", "completed") == "completed"
    ]
    if walk_forward_selections:
        lines.extend(["", "### Walk-forward 测试段汇总", ""])
        test_returns = [
            value
            for value in (
                _coerce_finite_number(row.get("test_total_return"))
                for row in walk_forward_selections
            )
            if value is not None
        ]
        test_sharpes = [
            value
            for value in (
                _coerce_finite_number(row.get("test_sharpe_ratio"))
                for row in walk_forward_selections
            )
            if value is not None
        ]
        test_drawdowns = [
            value
            for value in (
                _coerce_finite_number(row.get("test_max_drawdown"))
                for row in walk_forward_selections
            )
            if value is not None
        ]
        test_days = [
            value
            for value in (
                _coerce_finite_number(row.get("test_trading_days"))
                for row in walk_forward_selections
            )
            if value is not None
        ]
        _append_markdown_table(
            lines,
            ["指标", "数值"],
            [
                ("测试窗口数", len(walk_forward_selections)),
                ("测试收益均值", _format_metric(_mean(test_returns))),
                ("测试收益中位数", _format_metric(_median(test_returns))),
                (
                    "正收益窗口比例",
                    _format_percent(
                        sum(value > 0 for value in test_returns) / len(test_returns)
                        if test_returns
                        else None
                    ),
                ),
                ("测试夏普均值", _format_metric(_mean(test_sharpes))),
                ("测试夏普中位数", _format_metric(_median(test_sharpes))),
                (
                    "最差测试最大回撤",
                    _format_metric(min(test_drawdowns)) if test_drawdowns else "—",
                ),
                (
                    "测试交易日合计",
                    _format_report_value(sum(test_days) if test_days else None),
                ),
            ],
        )
        lines.append(
            "Walk-forward 汇总只统计各测试段的分布，不在缺少每日净值时伪造一条复合收益曲线。"
        )

    lines.extend(["", "## 7. 稳定性与风险提示", ""])
    warnings = _build_report_warnings(
        result,
        selected_training_rows,
        failed_training_rows,
        walk_forward_selections,
    )
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("本次没有触发预设的样本、权重集中或窗口数量提示。")

    lines.extend(["", "## 8. 研究结论与下一步", ""])
    if report_status == "protocol_failed":
        lines.append("研究协议门禁未通过；本次测试结果不能作为有效的策略证据。")
    elif report_status == "completed_with_rejections":
        rejected_count = sum(
            row.get("status") == "rejected" for row in result.selection_rows
        )
        lines.append(
            f"研究协议已按预设规则执行，有 {rejected_count} 个窗口因模型或验证证据不足而未读取测试段。"
        )
    elif report_status == "completed_with_exclusions":
        lines.append(
            f"本次评估完成，但有 {len(failed_training_rows)} 个训练组合被排除；入选结果只能在剩余成功组合范围内解释。"
        )
    elif report_status == "completed":
        lines.append("本次所有评估窗口均完成训练、验证选择和测试执行。")
    else:
        lines.append("本次评估没有为所有窗口生成有效的训练、验证和测试结果。")
    lines.append(
        "报告只描述本次时间切分和参数空间内的研究事实，不自动证明因子具有长期有效性，也不构成买入或实盘启用建议。"
    )
    if config.retrospective_method_development:
        lines.append(
            "本报告标记为回溯性方法开发：相关历史测试区间已在方法修复前被查看，因此不能作为新的盲测确认。"
        )
    lines.extend(
        [
            "",
            "建议下一步检查：",
            "- 扩大训练时间范围并确认配置化调仓信号日数量是否足够。",
            "- 对权重高度集中或窗口全部失败的因子组合单独做因子诊断。",
            "- 比较 Walk-forward 各窗口的入选候选和权重变化，而不是只看平均测试收益。",
            "- 使用独立测试配置复核入选方案，避免继续用同一验证集反复调参。",
        ]
    )

    lines.extend(["", "## 9. 审计文件", ""])
    _append_markdown_table(
        lines,
        ["文件", "内容"],
        [
            ("`parameters.json`", "完整研究配置、切分和搜索空间快照"),
            ("`training_models.csv`", "每个训练组合的样本、信号日、收敛状态和拟合权重"),
            (
                "`hyperparameter_trials.csv`",
                "每个网格组合的参数、拟合权重、训练/验证指标和失败原因",
            ),
            ("`candidate_metrics.csv`", "全部候选的训练/验证指标及入选组合测试指标"),
            ("`selections.csv`", "每个窗口的验证选择和测试结果"),
            (
                "`evaluation_failures.csv`",
                "训练/验证/测试回测或指标计算失败的组合与原因",
            ),
            (
                "`research_validity.csv`",
                "各窗口、候选和阶段的样本、模型与验证门禁实际值及拒绝原因",
            ),
            (
                "`research_protocol.csv`",
                "训练失败比例、选择负担、测试窗口独立性和完成窗口数门禁",
            ),
            (
                "`selection_diagnostics.csv`",
                "各窗口验证选择的比较规模、排名差距和多重比较风险",
            ),
            (
                "`factor_weight_diagnostics.csv`",
                "逐训练组合的权重数量、集中度、有效因子数和入选标记",
            ),
        ],
    )
    return "\n".join(lines) + "\n"


def _append_markdown_table(lines: list[str], headers, rows) -> None:
    lines.append("| " + " | ".join(_escape_markdown(value) for value in headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append(
            "| "
            + " | ".join(_escape_markdown(_format_report_value(value)) for value in row)
            + " |"
        )


def _escape_markdown(value: str) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _format_report_value(value) -> str:
    if value is None:
        return "—"
    if isinstance(value, str):
        return value or "—"
    try:
        if pd.isna(value):
            return "—"
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        if not math.isfinite(value):
            return "—"
        return f"{value:.6g}"
    return str(value)


def _format_metric(value) -> str:
    numeric = _coerce_finite_number(value)
    return "—" if numeric is None else f"{numeric:.4f}"


def _format_percent(value) -> str:
    numeric = _coerce_finite_number(value)
    return "—" if numeric is None else f"{numeric:.1%}"


def _coerce_finite_number(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _require_finite_selection_metric(
    metrics: dict, selection_metric: str, phase: str
) -> float:
    value = _coerce_finite_number(metrics.get(selection_metric))
    if value is None:
        raise ValueError(f"{phase} 的选择指标 {selection_metric} 缺失或非有限")
    return value


def _format_range(values: list[int | float]) -> str:
    if not values:
        return "—"
    low = min(values)
    high = max(values)
    return _format_report_value(low) if low == high else f"{low}~{high}"


def _format_period(period: EvaluationPeriod) -> str:
    return f"{period.start_date.isoformat()} ~ {period.end_date.isoformat()}"


def _format_split(split: EvaluationSplit) -> str:
    return (
        f"训练 {_format_period(split.train)}；"
        f"验证 {_format_period(split.validation)}；"
        f"测试 {_format_period(split.test)}"
    )


def _format_walk_forward(spec: WalkForwardSpec | None) -> str:
    if spec is None:
        return "未启用"
    return (
        f"{spec.train_years}年训练 + {spec.validation_years}年验证 + "
        f"{spec.test_years}年测试，步长 {spec.step_years}年，范围 "
        f"{spec.start_date.isoformat()} ~ {spec.end_date.isoformat()}"
    )


def _format_training_parameters(training: TrainingSpec | None) -> str:
    if training is None or not training.enabled:
        return "未启用"
    return (
        f"label_horizon_days={training.label_horizon_days}, "
        f"ridge_alpha={training.ridge_alpha}, "
        f"max_iterations={training.max_iterations}, "
        f"minimum_training_observations={training.minimum_training_observations}, "
        f"minimum_training_dates={training.minimum_training_dates}"
    )


def _format_validity_parameters(validity: ResearchValiditySpec | None) -> str:
    if validity is None or not validity.enabled:
        return "未启用"
    return (
        f"training_signal_dates>={validity.minimum_training_signal_dates}, "
        f"validation_signal_dates>={validity.minimum_validation_signal_dates}, "
        f"test_signal_dates>={validity.minimum_test_signal_dates}, "
        f"validation_observations>={validity.minimum_validation_observations}, "
        f"test_observations>={validity.minimum_test_observations}, "
        f"effective_factors>={validity.minimum_effective_factor_count}, "
        f"max_factor_weight<={validity.maximum_factor_weight}, "
        "validation_score>="
        f"{validity.minimum_validation_selection_score}, "
        "training_failure_ratio<="
        f"{validity.maximum_training_failure_ratio}, "
        "trials_per_validation_signal_date<="
        f"{validity.maximum_trials_per_validation_signal_date}, "
        f"completed_test_windows>={validity.minimum_completed_test_windows}, "
        "non_overlapping_tests="
        f"{validity.require_non_overlapping_test_windows}"
    )


def _format_search_parameters(search: HyperparameterSearchSpec | None) -> str:
    if search is None or not search.enabled:
        return "未启用"
    return json.dumps(search.to_dict(), ensure_ascii=False, sort_keys=True)


def _parse_json_mapping(value) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _build_factor_weight_diagnostic_rows(
    result: FactorExperimentEvaluationResult,
) -> list[dict]:
    selected_trial_ids = {
        (row.get("split_id"), selected_id)
        for row in result.selection_rows
        if (
            selected_id := row.get("selected_trial_id") or row.get("selected_candidate")
        )
    }
    diagnostic_rows = []
    for training_row in result.training_rows:
        weights = _parse_json_mapping(training_row.get("factor_weights"))
        factor_parameters = _parse_json_mapping(training_row.get("factor_parameters"))
        numeric_weights = {
            factor_name: numeric
            for factor_name, weight in weights.items()
            if (numeric := _coerce_finite_number(weight)) is not None and numeric >= 0
        }
        factor_count = len(weights) or len(factor_parameters) or None
        nonzero_weights = {
            factor_name: weight
            for factor_name, weight in numeric_weights.items()
            if weight > 1e-12
        }
        max_factor = (
            max(numeric_weights, key=numeric_weights.get) if numeric_weights else None
        )
        max_weight = max(numeric_weights.values()) if numeric_weights else None
        if numeric_weights:
            squared_sum = sum(weight**2 for weight in numeric_weights.values())
            effective_factor_count = 1 / squared_sum if squared_sum > 1e-12 else None
            positive_weights = [
                weight for weight in numeric_weights.values() if weight > 1e-12
            ]
            weight_entropy = -sum(
                weight * math.log(weight) for weight in positive_weights
            )
            normalized_weight_entropy = (
                weight_entropy / math.log(factor_count) if factor_count > 1 else 0.0
            )
            if len(nonzero_weights) <= 1 or max_weight >= 0.90:
                collapse_level = "single_factor"
            elif max_weight >= 0.60:
                collapse_level = "concentrated"
            else:
                collapse_level = "diversified"
        else:
            effective_factor_count = None
            weight_entropy = None
            normalized_weight_entropy = None
            collapse_level = "not_available"
        split_id = training_row.get("split_id")
        trial_id = training_row.get("trial_id")
        diagnostic_rows.append(
            {
                "split_id": split_id,
                "candidate_id": training_row.get("candidate_id"),
                "trial_id": trial_id,
                "status": training_row.get("status"),
                "error": training_row.get("error", ""),
                "selected": (split_id, trial_id) in selected_trial_ids,
                "factor_count": factor_count,
                "nonzero_factor_count": len(nonzero_weights),
                "max_factor": max_factor,
                "max_weight": max_weight,
                "effective_factor_count": effective_factor_count,
                "weight_entropy": weight_entropy,
                "normalized_weight_entropy": normalized_weight_entropy,
                "collapse_level": collapse_level,
            }
        )
    return diagnostic_rows


def _format_weights(weights: dict[str, object]) -> str:
    if not weights:
        return "—"
    formatted = []
    for factor_name, value in sorted(weights.items()):
        numeric = _coerce_finite_number(value)
        formatted.append(
            f"{factor_name}={numeric:.4f}"
            if numeric is not None
            else f"{factor_name}=—"
        )
    return "; ".join(formatted)


def _format_winsorize_range(selection: dict) -> str:
    lower = selection.get("selected_winsorize_lower")
    upper = selection.get("selected_winsorize_upper")
    if lower is None or upper is None:
        return "—"
    return f"{lower}~{upper}"


def _ordered_split_ids(config: FactorExperimentEvaluationConfig) -> list[str]:
    return [split.split_id for split in config.get_splits()]


def _get_report_status(
    result: FactorExperimentEvaluationResult, failed_count: int
) -> str:
    expected = set(_ordered_split_ids(result.config))
    actual = {row.get("split_id") for row in result.selection_rows}
    if not result.selection_rows or actual != expected:
        return "failed"
    if result.protocol_status == "protocol_failed":
        return "protocol_failed"
    statuses = {row.get("status", "completed") for row in result.selection_rows}
    if "failed" in statuses:
        return "failed"
    if "rejected" in statuses:
        return "completed_with_rejections"
    if failed_count or result.evaluation_failure_rows:
        return "completed_with_exclusions"
    return "completed"


def _determine_strategy_evidence_status(
    config: FactorExperimentEvaluationConfig,
    protocol_status: str,
    completed_test_selections: list[dict],
) -> str:
    if protocol_status == "protocol_failed":
        return "invalid_protocol"
    if config.retrospective_method_development:
        return "retrospective_descriptive_only"
    minimum_windows = (
        config.validity.minimum_completed_test_windows
        if config.validity is not None and config.validity.enabled
        else 1
    )
    if len(completed_test_selections) < minimum_windows:
        return "insufficient_test_windows"
    return "descriptive_test_evidence"


def _append_top_trials(
    lines: list[str], result: FactorExperimentEvaluationResult
) -> None:
    search = result.config.hyperparameter_search
    if search is None or not search.enabled:
        lines.append("未启用外层搜索。")
        return
    metric_field = f"validation_{result.config.selection_metric}"
    selected_trial_ids = {
        (row.get("split_id"), row.get("selected_trial_id"))
        for row in result.selection_rows
    }
    for split_id in _ordered_split_ids(result.config):
        rows = [
            row
            for row in result.hyperparameter_rows
            if row.get("split_id") == split_id
            and row.get("status") in {"fitted", "rejected"}
        ]
        rows.sort(
            key=lambda row: (
                _coerce_finite_number(row.get(metric_field))
                if _coerce_finite_number(row.get(metric_field)) is not None
                else (
                    -math.inf
                    if result.config.selection_direction == "max"
                    else math.inf
                )
            ),
            reverse=result.config.selection_direction == "max",
        )
        if not rows:
            lines.append(f"{split_id}：没有完成训练和验证的组合。")
            continue
        lines.append(f"**{split_id}**")
        lines.append("")
        _append_markdown_table(
            lines,
            [
                "候选",
                "试验",
                "因子参数",
                "持仓数",
                "缩尾范围",
                "Ridge",
                "训练指标",
                "验证指标",
                "验证决策",
            ],
            [
                (
                    row.get("candidate_id"),
                    row.get("trial_id"),
                    _format_json_value(row.get("factor_parameters")),
                    row.get("holding_count"),
                    f"{row.get('winsorize_lower')}~{row.get('winsorize_upper')}",
                    row.get("ridge_alpha"),
                    _format_metric(row.get(f"train_{result.config.selection_metric}")),
                    _format_metric(row.get(metric_field)),
                    "入选"
                    if (split_id, row.get("trial_id")) in selected_trial_ids
                    else "验证拒绝"
                    if row.get("status") == "rejected"
                    else "未入选",
                )
                for row in rows[:10]
            ],
        )
        if len(rows) > 10:
            lines.append("仅展示 Top 10，完整组合见 `hyperparameter_trials.csv`。")
        lines.append("")


def _format_json_value(value) -> str:
    if value is None or value == "":
        return "—"
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _find_selected_metric_row(
    metric_rows: tuple[dict, ...], selection: dict, phase: str
) -> dict | None:
    selected_id = selection.get("selected_trial_id") or selection.get(
        "selected_candidate"
    )
    for row in metric_rows:
        row_id = row.get("trial_id") or row.get("candidate_id")
        if (
            row.get("split_id") == selection.get("split_id")
            and row.get("phase") == phase
            and row_id == selected_id
        ):
            return row
    return None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _population_std(values: list[float]) -> float | None:
    if not values:
        return None
    average = _mean(values)
    if average is None:
        return None
    return math.sqrt(sum((value - average) ** 2 for value in values) / len(values))


def _build_report_warnings(
    result: FactorExperimentEvaluationResult,
    selected_training_rows: list[dict],
    failed_training_rows: list[dict],
    walk_forward_selections: list[dict],
) -> list[str]:
    warnings = []
    failed_validity_rows = [
        row for row in result.validity_rows if row.get("status") != "passed"
    ]
    if failed_validity_rows:
        warnings.append(
            f"有 {len(failed_validity_rows)} 条研究有效性门禁失败；相关窗口不能作为有效研究结论。"
        )
    if failed_training_rows:
        warnings.append(f"有 {len(failed_training_rows)} 个训练组合失败并被排除。")
    if result.evaluation_failure_rows:
        warnings.append(
            f"有 {len(result.evaluation_failure_rows)} 个训练/验证/测试执行失败，详情见 evaluation_failures.csv。"
        )
    weight_diagnostic_rows = _build_factor_weight_diagnostic_rows(result)
    for split_id in _ordered_split_ids(result.config):
        split_rows = [
            row
            for row in weight_diagnostic_rows
            if row.get("split_id") == split_id and row.get("status") == "fitted"
        ]
        if not split_rows:
            continue
        single_factor_rows = [
            row for row in split_rows if row.get("collapse_level") == "single_factor"
        ]
        selected_rows = [row for row in split_rows if row.get("selected")]
        selected_single_factor = any(
            row.get("collapse_level") == "single_factor" for row in selected_rows
        )
        if len(single_factor_rows) == len(split_rows):
            warnings.append(
                f"{split_id} 的全部 {len(split_rows)} 个成功训练组合都为 single_factor，"
                "权重塌缩是训练结果的普遍现象，不能只归因于验证集选择。"
            )
        elif selected_single_factor:
            warnings.append(
                f"{split_id} 的入选组合为 single_factor，但全部成功训练组合中只有 "
                f"{len(single_factor_rows)}/{len(split_rows)} 组如此，验证集选择可能放大权重集中。"
            )
    for diagnostic in result.selection_diagnostic_rows:
        if diagnostic.get("status") != "completed":
            continue
        split_id = diagnostic.get("split_id")
        signal_date_count = _coerce_finite_number(
            diagnostic.get("validation_signal_date_count")
        )
        trial_count = _coerce_finite_number(diagnostic.get("fitted_trial_count"))
        if (
            signal_date_count is not None
            and signal_date_count < REPORT_SIGNAL_DATE_WARNING_THRESHOLD
        ):
            warnings.append(
                f"{split_id} 验证集只有 {int(signal_date_count)} 个可执行信号日，低于 "
                f"{REPORT_SIGNAL_DATE_WARNING_THRESHOLD} 个提示阈值；验证排序和入选结果需要谨慎解释。"
            )
        if (
            signal_date_count is not None
            and trial_count is not None
            and trial_count > signal_date_count
        ):
            warnings.append(
                f"{split_id} 在 {int(signal_date_count)} 个验证信号日上比较了 "
                f"{int(trial_count)} 组组合，存在多重比较负担；验证高分不能视为无偏表现。"
            )
        tied_best_count = _coerce_finite_number(diagnostic.get("tied_best_trial_count"))
        if tied_best_count is not None and tied_best_count > 1:
            warnings.append(
                f"{split_id} 有 {int(tied_best_count)} 组组合在浮点容差内并列验证第一名，"
                "入选依赖稳定排序，差异不足以支持唯一方案。"
            )
    for row in selected_training_rows:
        split_id = row.get("split_id")
        signal_date_count = _coerce_finite_number(row.get("signal_date_count"))
        if (
            signal_date_count is not None
            and signal_date_count < REPORT_SIGNAL_DATE_WARNING_THRESHOLD
        ):
            warnings.append(
                f"{split_id} 只有 {int(signal_date_count)} 个训练信号日，低于 {REPORT_SIGNAL_DATE_WARNING_THRESHOLD} 个提示阈值；权重稳定性需要谨慎解释。"
            )
        weights = _parse_json_mapping(row.get("factor_weights"))
        numeric_weights = {
            factor_name: numeric
            for factor_name, weight in weights.items()
            if (numeric := _coerce_finite_number(weight)) is not None
        }
        if numeric_weights and max(numeric_weights.values()) >= 0.90:
            factor_name = max(numeric_weights, key=numeric_weights.get)
            warnings.append(
                f"{split_id} 的拟合权重高度集中在 {factor_name}（最大权重 {_format_metric(max(numeric_weights.values()))}）。"
            )
    completed_test_selections = [
        row
        for row in result.selection_rows
        if row.get("status", "completed") == "completed"
    ]
    if result.selection_rows and len(completed_test_selections) < 3:
        if walk_forward_selections:
            walk_forward_count = sum(
                row.get("status", "completed") == "completed"
                for row in walk_forward_selections
            )
            warnings.append(
                f"Walk-forward 只有 {walk_forward_count} 个有效测试窗口，"
                "跨窗口稳定性证据较弱。"
            )
        else:
            warnings.append(
                f"测试窗口只有 {len(completed_test_selections)} 个有效窗口，"
                "稳定性证据较弱。"
            )
    for selection in result.selection_rows:
        if selection.get("status", "completed") != "completed":
            continue
        train_metrics = _find_selected_metric_row(
            result.metric_rows, selection, "train"
        )
        validation_metrics = _find_selected_metric_row(
            result.metric_rows, selection, "validation"
        )
        if train_metrics and validation_metrics:
            train_score = _coerce_finite_number(
                train_metrics.get(result.config.selection_metric)
            )
            validation_score = _coerce_finite_number(
                validation_metrics.get(result.config.selection_metric)
            )
            if (
                train_score is not None
                and validation_score is not None
                and abs(train_score - validation_score)
                > REPORT_TRAIN_VALIDATION_GAP_THRESHOLD
            ):
                warnings.append(
                    f"{selection.get('split_id')} 的训练/验证 {result.config.selection_metric} 差值为 "
                    f"{abs(train_score - validation_score):.4f}，超过 "
                    f"{REPORT_TRAIN_VALIDATION_GAP_THRESHOLD:.2f} 提示阈值。"
                )
        test_drawdown = _coerce_finite_number(selection.get("test_max_drawdown"))
        test_signal_date_count = _coerce_finite_number(
            selection.get("test_signal_date_count")
        )
        if (
            test_signal_date_count is not None
            and test_signal_date_count < REPORT_SIGNAL_DATE_WARNING_THRESHOLD
        ):
            warnings.append(
                f"{selection.get('split_id')} 测试段只有 {int(test_signal_date_count)} 个可执行信号日，"
                "测试证据有限，即使测试结果为正也不能据此确认长期有效。"
            )
        if (
            test_drawdown is not None
            and test_drawdown <= REPORT_EXTREME_DRAWDOWN_THRESHOLD
        ):
            warnings.append(
                f"{selection.get('split_id')} 测试段最大回撤为 {_format_percent(test_drawdown)}，"
                f"达到或超过 {_format_percent(REPORT_EXTREME_DRAWDOWN_THRESHOLD)} 风险提示阈值。"
            )

    if (
        result.config.hyperparameter_search is not None
        and result.config.hyperparameter_search.enabled
    ):
        fitted_rows = [
            row for row in result.hyperparameter_rows if row.get("status") == "fitted"
        ]
        for split_id in _ordered_split_ids(result.config):
            split_rows = [row for row in fitted_rows if row.get("split_id") == split_id]
            grouped = {}
            for row in split_rows:
                key = (
                    row.get("candidate_id"),
                    row.get("holding_count"),
                    row.get("winsorize_lower"),
                    row.get("winsorize_upper"),
                    row.get("factor_parameters"),
                )
                grouped.setdefault(key, []).append(row)
            for rows in grouped.values():
                ridge_values = {row.get("ridge_alpha") for row in rows}
                weight_values = {row.get("factor_weights", "") for row in rows}
                metric_values = {
                    row.get(f"validation_{result.config.selection_metric}")
                    for row in rows
                }
                if (
                    len(ridge_values) > 1
                    and len(weight_values) == 1
                    and len(metric_values) == 1
                ):
                    warnings.append(
                        f"{split_id} 中不同 ridge_alpha 产生了相同拟合权重和验证指标，"
                        "该参数维度可能没有实际区分度。"
                    )
                    break

        for (
            factor_name,
            parameter_values,
        ) in result.config.hyperparameter_search.factor_parameter_values.items():
            for parameter_name, values in parameter_values.items():
                for value in values:
                    for split_id in _ordered_split_ids(result.config):
                        matching_rows = [
                            row
                            for row in fitted_rows
                            if row.get("split_id") == split_id
                            and _parse_json_mapping(row.get("factor_parameters"))
                            .get(factor_name, {})
                            .get(parameter_name)
                            == value
                        ]
                        all_rows = [
                            row
                            for row in result.hyperparameter_rows
                            if row.get("split_id") == split_id
                            and _parse_json_mapping(row.get("factor_parameters"))
                            .get(factor_name, {})
                            .get(parameter_name)
                            == value
                        ]
                        if all_rows and not matching_rows:
                            warnings.append(
                                f"{split_id} 中因子 {factor_name} 的 "
                                f"{parameter_name}={value} 组合全部失败，"
                                "无法评价该因子窗口。"
                            )

    if len(walk_forward_selections) > 1:
        candidates = {row.get("selected_candidate") for row in walk_forward_selections}
        if len(candidates) > 1:
            warnings.append(
                "Walk-forward 各窗口入选候选发生切换，组合稳定性需要谨慎解释。"
            )
        training_by_split = {
            row.get("split_id"): row
            for row in selected_training_rows
            if str(row.get("split_id", "")).startswith("walk_forward_")
        }
        weight_signatures = {
            row.get("factor_weights", "")
            for row in (
                training_by_split.get(selection.get("split_id"), {})
                for selection in walk_forward_selections
            )
            if row
        }
        if len(weight_signatures) > 1:
            warnings.append(
                "Walk-forward 各窗口拟合权重发生切换，不能仅用平均权重代表全部窗口。"
            )
    return list(dict.fromkeys(warnings))
