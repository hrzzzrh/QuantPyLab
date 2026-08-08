from dataclasses import asdict, dataclass, field, replace
from datetime import date
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class BacktestConfig:
    start_date: date
    end_date: date
    strategy_name: str
    strategy_version: str = ""
    strategy_parameters: dict = field(default_factory=dict)
    initial_capital: float = 1_000_000.0
    commission_bps: float = 5.0
    slippage_bps: float = 5.0
    benchmark_symbol: str | None = "510300"

    def __post_init__(self):
        if self.start_date >= self.end_date:
            raise ValueError("回测开始日期必须早于结束日期")
        if not self.strategy_name:
            raise ValueError("策略名称不能为空")
        if self.initial_capital <= 0:
            raise ValueError("初始资金必须为正数")
        if self.commission_bps < 0 or self.slippage_bps < 0:
            raise ValueError("手续费和滑点不能为负数")
        if self.benchmark_symbol and (
            not isinstance(self.benchmark_symbol, str) or not self.benchmark_symbol.isdigit()
        ):
            raise ValueError("基准代码必须是纯数字")

    @property
    def transaction_cost_rate(self) -> float:
        return (self.commission_bps + self.slippage_bps) / 10_000

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["start_date"] = self.start_date.isoformat()
        payload["end_date"] = self.end_date.isoformat()
        payload["transaction_cost_rate"] = self.transaction_cost_rate
        return payload

    def with_resolved_strategy(self, version: str, parameters: dict) -> "BacktestConfig":
        return replace(self, strategy_version=version, strategy_parameters=parameters)


def load_backtest_config(config_path: str | Path) -> BacktestConfig:
    path = Path(config_path)
    with path.open("rb") as file:
        document = tomllib.load(file)

    run = document.get("run")
    strategy = document.get("strategy")
    if not isinstance(run, dict) or not isinstance(strategy, dict):
        raise ValueError("回测配置必须包含 [run] 和 [strategy] 区段")
    parameters = strategy.get("parameters", {})
    if not isinstance(parameters, dict):
        raise ValueError("[strategy.parameters] 必须是 TOML 表")

    try:
        return BacktestConfig(
            start_date=date.fromisoformat(run["start_date"]),
            end_date=date.fromisoformat(run["end_date"]),
            strategy_name=strategy["name"],
            strategy_version=strategy.get("version", ""),
            strategy_parameters=parameters,
            initial_capital=run.get("initial_capital", 1_000_000.0),
            commission_bps=run.get("commission_bps", 5.0),
            slippage_bps=run.get("slippage_bps", 5.0),
            benchmark_symbol=run.get("benchmark_symbol", "510300"),
        )
    except KeyError as error:
        raise ValueError(f"回测配置缺少必填字段: {error.args[0]}") from error
