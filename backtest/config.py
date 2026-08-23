import tomllib
from dataclasses import asdict, dataclass, field, replace
from datetime import date
from pathlib import Path

BIWEEKLY_REBALANCE_ANCHOR_DATE = date(1970, 1, 5)
SUPPORTED_REBALANCE_FREQUENCIES = frozenset(
    {"monthly", "weekly", "biweekly", "every_n_trading_days"}
)
REBALANCE_TRADE_REASONS = frozenset(
    f"{frequency}_rebalance" for frequency in SUPPORTED_REBALANCE_FREQUENCIES
)


def validate_rebalance_schedule_parameters(
    rebalance_frequency: str,
    rebalance_interval_trading_days: int | None,
) -> None:
    """Validate the shared rebalance schedule configuration."""

    if rebalance_frequency not in SUPPORTED_REBALANCE_FREQUENCIES:
        supported = ", ".join(sorted(SUPPORTED_REBALANCE_FREQUENCIES))
        raise ValueError(f"调仓频率必须是以下之一: {supported}")
    if rebalance_frequency == "every_n_trading_days":
        if (
            isinstance(rebalance_interval_trading_days, bool)
            or not isinstance(rebalance_interval_trading_days, int)
            or rebalance_interval_trading_days <= 0
        ):
            raise ValueError(
                "every_n_trading_days 必须设置正整数 rebalance_interval_trading_days"
            )
    elif rebalance_interval_trading_days is not None:
        raise ValueError("rebalance_interval_trading_days 仅用于 every_n_trading_days")


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
    rebalance_frequency: str = "monthly"
    rebalance_interval_trading_days: int | None = None

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
            not isinstance(self.benchmark_symbol, str)
            or not self.benchmark_symbol.isdigit()
        ):
            raise ValueError("基准代码必须是纯数字")
        validate_rebalance_schedule_parameters(
            self.rebalance_frequency,
            self.rebalance_interval_trading_days,
        )

    @property
    def transaction_cost_rate(self) -> float:
        return (self.commission_bps + self.slippage_bps) / 10_000

    @property
    def rebalance_trade_reason(self) -> str:
        return f"{self.rebalance_frequency}_rebalance"

    @property
    def rebalance_anchor_kind(self) -> str:
        return resolve_rebalance_anchor_metadata(
            self.rebalance_frequency,
            self.start_date,
        )[0]

    @property
    def rebalance_anchor_date(self) -> str | None:
        return resolve_rebalance_anchor_metadata(
            self.rebalance_frequency,
            self.start_date,
        )[1]

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["start_date"] = self.start_date.isoformat()
        payload["end_date"] = self.end_date.isoformat()
        payload["transaction_cost_rate"] = self.transaction_cost_rate
        payload["rebalance_anchor_kind"] = self.rebalance_anchor_kind
        payload["rebalance_anchor_date"] = self.rebalance_anchor_date
        return payload

    def with_resolved_strategy(
        self, version: str, parameters: dict
    ) -> "BacktestConfig":
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
            rebalance_frequency=run.get("rebalance_frequency", "monthly"),
            rebalance_interval_trading_days=run.get("rebalance_interval_trading_days"),
        )
    except KeyError as error:
        raise ValueError(f"回测配置缺少必填字段: {error.args[0]}") from error


def resolve_rebalance_anchor_metadata(
    rebalance_frequency: str,
    start_date: date,
) -> tuple[str, str | None]:
    """Return the persisted anchor semantics for one rebalance schedule."""

    if rebalance_frequency == "monthly":
        return "calendar_month", None
    if rebalance_frequency == "weekly":
        return "calendar_week_monday_to_sunday", None
    if rebalance_frequency == "biweekly":
        return "fixed_biweekly_calendar", BIWEEKLY_REBALANCE_ANCHOR_DATE.isoformat()
    if rebalance_frequency == "every_n_trading_days":
        return "start_date", start_date.isoformat()
    raise ValueError(f"无法解析未知调仓频率的锚点: {rebalance_frequency}")
