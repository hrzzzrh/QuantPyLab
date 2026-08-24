"""Train the latest deployable factor model after historical research validation."""

from __future__ import annotations

import json
import math
import tomllib
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path
from statistics import median, pstdev

import pandas as pd

from analysis.factors import FactorEngine
from analysis.factors.transforms import filter_valid_factor_rows
from backtest.config import BacktestConfig, load_backtest_config
from backtest.data_access import BacktestDataAccess
from backtest.factor_trainer import FactorTrainingResult
from backtest.hyperparameter_search import (
    HyperparameterTrial,
    expand_hyperparameter_trials,
)
from backtest.research_evaluator import (
    EvaluationPeriod,
    FactorExperimentEvaluationConfig,
    FactorExperimentEvaluationResult,
    build_research_reproducibility_audit,
    evaluate_factor_experiments,
    fit_factor_candidate_config,
    load_factor_experiment_evaluation_config,
    validate_factor_training_result,
    write_factor_experiment_evaluation_report,
)
from backtest.research_provenance import (
    build_research_data_snapshot,
    verify_research_data_snapshot_unchanged,
)
from backtest.strategy_registry import get_backtest_strategy
from storage.database.manager import DBManager


@dataclass(frozen=True)
class ProductionTrainingSpec:
    """Latest rolling-refit policy after the historical protocol passes."""

    training_years: int = 6
    minimum_validation_windows: int = 3

    def __post_init__(self):
        for name in ("training_years", "minimum_validation_windows"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"[production].{name} 必须是正整数")

    def to_dict(self) -> dict[str, int]:
        return {
            "training_years": self.training_years,
            "minimum_validation_windows": self.minimum_validation_windows,
        }


@dataclass(frozen=True)
class FactorProductionTrainingConfig:
    research: FactorExperimentEvaluationConfig
    production: ProductionTrainingSpec

    def __post_init__(self):
        if self.research.training is None or not self.research.training.enabled:
            raise ValueError("生产模型训练要求研究配置启用 [training]")
        if self.research.validity is None or not self.research.validity.enabled:
            raise ValueError("生产模型训练要求研究配置启用 [validity]")
        if self.research.hyperparameter_search is None:
            raise ValueError("生产参数锁定要求研究配置启用 [hyperparameter_search]")
        if not self.research.training.refit_selected_on_train_validation:
            raise ValueError(
                "生产研究配置必须启用 [training].refit_selected_on_train_validation"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "research": self.research.to_dict(),
            "production": self.production.to_dict(),
        }


@dataclass(frozen=True)
class FactorProductionTrainingResult:
    config: FactorProductionTrainingConfig
    as_of_date: date
    latest_market_date: date
    label_complete_date: date
    production_train_start_date: date
    next_execution_date: date | None
    locked_trial: HyperparameterTrial
    validation_selection_rows: tuple[dict[str, object], ...]
    trained_config: BacktestConfig
    training_result: FactorTrainingResult
    targets: pd.DataFrame
    research_result: FactorExperimentEvaluationResult
    data_snapshot: dict[str, object]


def load_factor_production_training_config(
    config_path: str | Path,
) -> FactorProductionTrainingConfig:
    """Load one research TOML plus its required production-refit section."""

    path = Path(config_path)
    research = load_factor_experiment_evaluation_config(path)
    with path.open("rb") as file:
        document = tomllib.load(file)
    production = document.get("production")
    if not isinstance(production, dict):
        raise ValueError("生产模型配置必须包含 [production] 区段")
    return FactorProductionTrainingConfig(
        research=research,
        production=ProductionTrainingSpec(
            training_years=production.get("training_years", 6),
            minimum_validation_windows=production.get("minimum_validation_windows", 3),
        ),
    )


def build_production_trials(
    config: FactorExperimentEvaluationConfig,
) -> tuple[HyperparameterTrial, ...]:
    """Expand the exact candidate set used by the historical evaluator."""

    candidate_configs = [
        (path.stem, load_backtest_config(path)) for path in config.candidate_configs
    ]
    if config.hyperparameter_search is not None:
        return expand_hyperparameter_trials(
            candidate_configs,
            config.hyperparameter_search,
        )
    training = config.training
    trials = []
    for candidate_id, candidate_config in candidate_configs:
        resolved = get_backtest_strategy(
            candidate_config.strategy_name
        ).validate_parameters(candidate_config.strategy_parameters)
        trials.append(
            HyperparameterTrial(
                trial_id=candidate_id,
                candidate_id=candidate_id,
                config=candidate_config,
                parameters={
                    "holding_count": resolved.get("holding_count"),
                    "winsorize_lower": resolved.get("winsorize_lower"),
                    "winsorize_upper": resolved.get("winsorize_upper"),
                    "ridge_alpha": training.ridge_alpha if training else None,
                    "portfolio_weighting": resolved.get("portfolio_weighting", "equal"),
                    "factor_parameters": resolved.get("factor_parameters", {}),
                },
            )
        )
    return tuple(trials)


def select_locked_production_trial(
    research_result: FactorExperimentEvaluationResult,
    trials: tuple[HyperparameterTrial, ...],
    minimum_validation_windows: int,
) -> tuple[HyperparameterTrial, tuple[dict[str, object], ...]]:
    """Lock outer parameters using validation evidence only, never test metrics."""

    if research_result.protocol_status != "protocol_passed":
        raise ValueError(
            f"历史研究协议未通过，拒绝生成生产模型: {research_result.protocol_status}"
        )
    if (
        isinstance(minimum_validation_windows, bool)
        or not isinstance(minimum_validation_windows, int)
        or minimum_validation_windows <= 0
    ):
        raise ValueError("minimum_validation_windows 必须是正整数")

    config = research_result.config
    target_split_ids = [
        split.split_id
        for split in config.get_splits()
        if config.walk_forward is None or split.split_id.startswith("walk_forward_")
    ]
    if minimum_validation_windows > len(target_split_ids):
        raise ValueError(
            "生产参数要求的验证窗口数超过可用窗口: "
            f"{minimum_validation_windows} > {len(target_split_ids)}"
        )
    metric_column = f"validation_{config.selection_metric}"
    rows_by_trial: dict[str, list[dict]] = {trial.trial_id: [] for trial in trials}
    for row in research_result.hyperparameter_rows:
        trial_id = str(row.get("trial_id", ""))
        if trial_id in rows_by_trial and row.get("split_id") in target_split_ids:
            rows_by_trial[trial_id].append(row)

    ranking_rows = []
    for trial in trials:
        scores_by_split = {}
        for row in rows_by_trial[trial.trial_id]:
            if row.get("status") != "fitted":
                continue
            raw_score = row.get(metric_column)
            if raw_score is None:
                continue
            score = float(raw_score)
            if math.isfinite(score):
                scores_by_split[str(row["split_id"])] = score
        scores = [
            scores_by_split[split_id]
            for split_id in target_split_ids
            if split_id in scores_by_split
        ]
        passed_count = len(scores)
        eligible = passed_count >= minimum_validation_windows
        if scores:
            median_score = float(median(scores))
            mean_score = float(sum(scores) / len(scores))
            worst_score = (
                float(min(scores))
                if config.selection_direction == "max"
                else float(max(scores))
            )
            score_std = float(pstdev(scores)) if len(scores) > 1 else 0.0
        else:
            median_score = None
            mean_score = None
            worst_score = None
            score_std = None
        ranking_rows.append(
            {
                "candidate_id": trial.candidate_id,
                "trial_id": trial.trial_id,
                "eligible": eligible,
                "validation_passed_window_count": passed_count,
                "validation_expected_window_count": len(target_split_ids),
                "validation_score_median": median_score,
                "validation_score_mean": mean_score,
                "validation_score_worst": worst_score,
                "validation_score_std": score_std,
                "portfolio_weighting": trial.parameters.get(
                    "portfolio_weighting", "equal"
                ),
                "validation_scores_by_split": json.dumps(
                    scores_by_split,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "selected": False,
            }
        )

    def ranking_key(row: dict[str, object]) -> tuple:
        eligible_rank = 0 if row["eligible"] else 1
        passed_rank = -int(row["validation_passed_window_count"])
        median_score = row["validation_score_median"]
        worst_score = row["validation_score_worst"]
        if median_score is None or worst_score is None:
            metric_rank = (math.inf, math.inf)
        elif config.selection_direction == "max":
            metric_rank = (-float(median_score), -float(worst_score))
        else:
            metric_rank = (float(median_score), float(worst_score))
        return (eligible_rank, passed_rank, *metric_rank, str(row["trial_id"]))

    ranking_rows.sort(key=ranking_key)
    eligible_rows = [row for row in ranking_rows if row["eligible"]]
    if not eligible_rows:
        raise ValueError("没有候选达到生产参数最少验证窗口门槛，拒绝生成生产模型")
    selected_trial_id = str(eligible_rows[0]["trial_id"])
    for rank, row in enumerate(ranking_rows, start=1):
        row["rank"] = rank
        row["selected"] = row["trial_id"] == selected_trial_id
    trial_map = {trial.trial_id: trial for trial in trials}
    return trial_map[selected_trial_id], tuple(ranking_rows)


def resolve_production_training_dates(
    trading_dates: pd.DatetimeIndex,
    *,
    as_of_date: date,
    label_horizon_days: int,
    training_years: int,
) -> tuple[date, date, date, date | None]:
    """Resolve latest data, label-complete cutoff, rolling start, and T+1 date."""

    normalized = pd.DatetimeIndex(pd.to_datetime(trading_dates, errors="coerce"))
    if normalized.isna().any():
        raise ValueError("交易日历包含无效日期")
    normalized = normalized.drop_duplicates().sort_values().normalize()
    available = normalized[normalized.date <= as_of_date]
    if len(available) <= label_horizon_days:
        raise ValueError(
            "交易日历不足以形成完整训练标签: "
            f"available={len(available)}, label_horizon_days={label_horizon_days}"
        )
    latest_market_date = available[-1].date()
    label_complete_date = available[-(label_horizon_days + 1)].date()
    training_start_date = _subtract_years(label_complete_date, training_years)
    future_dates = normalized[normalized.date > latest_market_date]
    next_execution_date = future_dates[0].date() if len(future_dates) else None
    return (
        latest_market_date,
        label_complete_date,
        training_start_date,
        next_execution_date,
    )


def train_factor_production_model(
    config: FactorProductionTrainingConfig,
    database_manager: DBManager,
    *,
    as_of_date: date,
) -> FactorProductionTrainingResult:
    """Validate the method, lock parameters, refit latest weights, and build targets."""

    start_snapshot = build_research_data_snapshot(database_manager)
    research_result = evaluate_factor_experiments(config.research, database_manager)
    trials = build_production_trials(config.research)
    locked_trial, validation_rows = select_locked_production_trial(
        research_result,
        trials,
        config.production.minimum_validation_windows,
    )
    trading_dates = BacktestDataAccess(database_manager).load_trading_dates()
    training = config.research.training
    validity = config.research.validity
    if training is None or validity is None:
        raise RuntimeError("生产模型配置缺少训练或有效性协议")
    (
        latest_market_date,
        label_complete_date,
        training_start_date,
        next_execution_date,
    ) = resolve_production_training_dates(
        trading_dates,
        as_of_date=as_of_date,
        label_horizon_days=training.label_horizon_days,
        training_years=config.production.training_years,
    )
    trained_config, training_result = fit_factor_candidate_config(
        locked_trial.config,
        EvaluationPeriod(training_start_date, latest_market_date),
        training,
        database_manager,
        ridge_alpha=float(locked_trial.parameters["ridge_alpha"]),
    )
    validate_factor_training_result(training_result, training, validity)
    actual_signal_end = pd.to_datetime(training_result.signal_date_end, errors="coerce")
    if pd.isna(actual_signal_end) or actual_signal_end.date() > label_complete_date:
        raise ValueError(
            "生产训练实际最后信号日超过标签完整截止日: "
            f"cutoff={label_complete_date}, actual={training_result.signal_date_end}"
        )
    targets = build_initial_deployment_targets(
        trained_config,
        database_manager,
        signal_date=latest_market_date,
        next_execution_date=next_execution_date,
    )
    data_snapshot = verify_research_data_snapshot_unchanged(
        start_snapshot,
        build_research_data_snapshot(database_manager),
    )
    return FactorProductionTrainingResult(
        config=config,
        as_of_date=as_of_date,
        latest_market_date=latest_market_date,
        label_complete_date=label_complete_date,
        production_train_start_date=training_start_date,
        next_execution_date=next_execution_date,
        locked_trial=locked_trial,
        validation_selection_rows=validation_rows,
        trained_config=trained_config,
        training_result=training_result,
        targets=targets,
        research_result=research_result,
        data_snapshot=data_snapshot,
    )


def build_initial_deployment_targets(
    trained_config: BacktestConfig,
    database_manager: DBManager,
    *,
    signal_date: date,
    next_execution_date: date | None,
) -> pd.DataFrame:
    """Build a one-off from-cash target on the latest complete close."""

    strategy = get_backtest_strategy(trained_config.strategy_name)
    parameters = strategy.validate_parameters(trained_config.strategy_parameters)
    if not callable(getattr(strategy, "build_targets_from_candidates", None)):
        raise ValueError("生产目标当前只支持可从候选表生成目标的因子策略")
    target_config = replace(
        trained_config,
        start_date=signal_date - timedelta(days=1),
        end_date=signal_date,
        strategy_version="",
    )
    factor_names = tuple(parameters["factor_weights"])
    signal_data = BacktestDataAccess(database_manager).load_factor_data(
        target_config,
        factor_names,
        factor_parameters=parameters["factor_parameters"],
        minimum_history_days=parameters["min_listing_days"],
        data_end_date=signal_date,
        financial_signal_dates_only=False,
    )
    normalized_signal_date = pd.Timestamp(signal_date)
    factor_frame = FactorEngine().calculate_factors_on_dates(
        signal_data,
        factor_names,
        parameters["factor_parameters"],
        pd.DatetimeIndex([normalized_signal_date]),
        symbol_batch_size=125,
    )
    ordered = signal_data.loc[:, ["date", "symbol"]].copy()
    ordered["date"] = pd.to_datetime(ordered["date"])
    ordered = ordered.sort_values(["symbol", "date"], kind="mergesort")
    ordered["listing_days"] = ordered.groupby("symbol", sort=False).cumcount() + 1
    candidates = ordered.loc[
        ordered["date"].eq(normalized_signal_date)
        & (ordered["listing_days"] >= parameters["min_listing_days"]),
        ["date", "symbol", "listing_days"],
    ].merge(
        factor_frame.loc[:, ["date", "symbol", *factor_names]],
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    candidates = filter_valid_factor_rows(candidates, factor_names)
    targets = strategy.build_targets_from_candidates(candidates, parameters)
    if targets.empty:
        raise ValueError(f"最新信号日 {signal_date} 没有可用生产目标")
    targets = targets.copy()
    targets["target_kind"] = "initial_deployment"
    targets["earliest_execution_date"] = (
        next_execution_date.isoformat()
        if next_execution_date is not None
        else "next_trading_day"
    )
    return targets


def write_factor_production_training_report(
    result: FactorProductionTrainingResult,
    output_dir: str | Path,
) -> Path:
    """Write the production model, validation-only ranking, targets, and audit."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=False)
    write_factor_experiment_evaluation_report(
        result.research_result,
        output_path / "research_evaluation",
    )
    pd.DataFrame(result.validation_selection_rows).to_csv(
        output_path / "validation_selection.csv",
        index=False,
    )
    result.targets.to_csv(output_path / "production_targets.csv", index=False)
    training_details = result.training_result.to_dict()
    pd.DataFrame(
        [
            {
                "production_train_start_date": result.production_train_start_date,
                "latest_market_date": result.latest_market_date,
                "label_complete_date": result.label_complete_date,
                "signal_date_start": training_details["signal_date_start"],
                "signal_date_end": training_details["signal_date_end"],
                "observation_count": training_details["observation_count"],
                "signal_date_count": training_details["signal_date_count"],
                "label_horizon_days": training_details["label_horizon_days"],
            }
        ]
    ).to_csv(output_path / "training_summary.csv", index=False)
    model_payload = {
        "status": "deployable_initial_target",
        "strategy_name": result.trained_config.strategy_name,
        "strategy_version": result.trained_config.strategy_version,
        "as_of_date": result.as_of_date.isoformat(),
        "latest_market_date": result.latest_market_date.isoformat(),
        "label_complete_date": result.label_complete_date.isoformat(),
        "production_train_start_date": (result.production_train_start_date.isoformat()),
        "next_execution_date": (
            result.next_execution_date.isoformat()
            if result.next_execution_date is not None
            else "next_trading_day"
        ),
        "locked_candidate_id": result.locked_trial.candidate_id,
        "locked_trial_id": result.locked_trial.trial_id,
        "locked_parameters": result.locked_trial.parameters,
        "strategy_parameters": result.trained_config.strategy_parameters,
        "training_result": training_details,
        "target_semantics": (
            "首次从空仓上线的初始化目标；已有账户在非调仓日不得据此生成差额订单"
        ),
    }
    (output_path / "production_model.json").write_text(
        json.dumps(model_payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    audit = build_research_reproducibility_audit(
        result.config.research,
        result.data_snapshot,
    )
    (output_path / "parameters.json").write_text(
        json.dumps(
            {
                **result.config.to_dict(),
                "as_of_date": result.as_of_date.isoformat(),
                "latest_market_date": result.latest_market_date.isoformat(),
                "label_complete_date": result.label_complete_date.isoformat(),
                "production_train_start_date": (
                    result.production_train_start_date.isoformat()
                ),
                "protocol_status": result.research_result.protocol_status,
                "strategy_evidence_status": (
                    result.research_result.strategy_evidence_status
                ),
                "audit": audit,
            },
            ensure_ascii=False,
            indent=2,
            default=_json_default,
        ),
        encoding="utf-8",
    )
    (output_path / "summary.md").write_text(
        _build_production_summary(result, audit),
        encoding="utf-8",
    )
    return output_path


def _build_production_summary(
    result: FactorProductionTrainingResult,
    audit: dict[str, object],
) -> str:
    resource = audit.get("resource", {})
    weights = result.training_result.factor_weights
    lines = [
        "# 因子策略生产模型报告",
        "",
        "## 结论",
        "",
        "- 模型状态：可用于首次从空仓生成初始化目标",
        f"- 历史研究协议：`{result.research_result.protocol_status}`",
        f"- 策略证据状态：`{result.research_result.strategy_evidence_status}`",
        f"- 数据截至：{result.latest_market_date}",
        f"- 标签完整截止：{result.label_complete_date}",
        f"- 生产训练窗口：{result.production_train_start_date}—{result.latest_market_date}",
        f"- 实际训练信号：{result.training_result.signal_date_start}—{result.training_result.signal_date_end}",
        f"- 锁定方案：`{result.locked_trial.trial_id}`",
        f"- 最早执行：{result.next_execution_date or '下一交易日'}",
        "",
        "## 最新因子权重",
        "",
        "| 因子 | 权重 |",
        "|:---|---:|",
    ]
    lines.extend(f"| `{name}` | {weight:.6f} |" for name, weight in weights.items())
    lines.extend(
        [
            "",
            "## 使用边界",
            "",
            "`production_targets.csv` 是首次上线的初始化目标，不是成交记录。若账户已经运行，非调仓日应维持原持仓；没有真实账户持仓时，本报告不能计算差额订单。财务和行情仍遵守收盘后形成信号、下一交易日执行的 T+1 口径。",
            "",
            "## 资源与溯源",
            "",
            f"- 数据快照：`{result.data_snapshot.get('snapshot_id', 'unavailable')}`",
            f"- 运行期间快照一致：{result.data_snapshot.get('verified_unchanged_during_run')}",
            f"- 峰值 RSS：{resource.get('peak_rss_bytes')}",
            f"- 超过研究预算：{resource.get('peak_rss_exceeded')}",
            "",
        ]
    )
    return "\n".join(lines)


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _json_default(value):
    if isinstance(value, (date, pd.Timestamp)):
        return value.isoformat()
    raise TypeError(f"无法序列化类型: {type(value).__name__}")
