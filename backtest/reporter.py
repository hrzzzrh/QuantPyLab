import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from backtest.config import BacktestConfig
from backtest.metrics import calculate_performance_metrics


def write_backtest_result(
    config: BacktestConfig,
    daily_nav: pd.DataFrame,
    targets: pd.DataFrame,
    trades: pd.DataFrame,
    output_root: Path = Path("workspace/backtest/results"),
) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = output_root / f"{config.strategy_name}_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=False)

    (output_dir / "parameters.json").write_text(
        json.dumps(config.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    daily_nav.to_csv(output_dir / "daily_nav.csv", index=False)
    targets.to_csv(output_dir / "rebalance_targets.csv", index=False)
    trades.to_csv(output_dir / "trades.csv", index=False)
    _write_summary(output_dir / "summary.md", daily_nav, trades)
    return output_dir


def _write_summary(path: Path, daily_nav: pd.DataFrame, trades: pd.DataFrame) -> None:
    metrics = calculate_performance_metrics(daily_nav)
    benchmark_metrics = calculate_performance_metrics(daily_nav, "benchmark_nav")
    turnover = (
        trades.loc[trades["side"].isin(["BUY", "SELL"]), "notional"].sum()
        / daily_nav["nav"].iloc[0]
    )
    lines = [
        "# 回测摘要",
        "",
        "| 指标 | 策略 | 基准 |",
        "|:---|---:|---:|",
        f"| 总收益率 | {_format_percent(metrics['total_return'])} | {_format_percent(benchmark_metrics['total_return'])} |",
        f"| 年化收益率 | {_format_percent(metrics['annualized_return'])} | {_format_percent(benchmark_metrics['annualized_return'])} |",
        f"| 年化波动率 | {_format_percent(metrics['annualized_volatility'])} | {_format_percent(benchmark_metrics['annualized_volatility'])} |",
        f"| 夏普比率 | {_format_number(metrics['sharpe_ratio'])} | {_format_number(benchmark_metrics['sharpe_ratio'])} |",
        f"| 最大回撤 | {_format_percent(metrics['max_drawdown'])} | {_format_percent(benchmark_metrics['max_drawdown'])} |",
        f"| 交易日 | {metrics['trading_days']} | {benchmark_metrics['trading_days']} |",
        "",
        f"累计换手（单边名义金额/初始净值）：{turnover:.2f}",
        f"交易记录数：{len(trades)}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _format_percent(value):
    return "—" if value is None else f"{value:.2%}"


def _format_number(value):
    return "—" if value is None else f"{value:.2f}"
