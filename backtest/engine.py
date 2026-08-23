from collections.abc import Mapping
from dataclasses import dataclass

import pandas as pd

from backtest.config import BacktestConfig


@dataclass
class BacktestResult:
    daily_nav: pd.DataFrame
    trades: pd.DataFrame


@dataclass(frozen=True)
class PreparedMarketData:
    """Market structures shared by runs with the same price interval."""

    price_data: pd.DataFrame
    calendar: pd.DatetimeIndex
    price_map: dict


class DailyBacktestEngine:
    def __init__(self, config: BacktestConfig):
        self.config = config

    @staticmethod
    def prepare_market_data(
        prices: pd.DataFrame, config: BacktestConfig
    ) -> PreparedMarketData:
        required_columns = {"date", "symbol", "open", "open_hfq", "close_hfq"}
        missing_columns = required_columns - set(prices.columns)
        if missing_columns:
            raise ValueError(f"行情数据缺少字段: {', '.join(sorted(missing_columns))}")

        price_columns = ["date", "symbol", "open", "open_hfq", "close_hfq"]
        if "raw_close" in prices.columns:
            price_columns.append("raw_close")
        price_data = prices.loc[:, price_columns].copy()
        price_data["date"] = pd.to_datetime(price_data["date"])
        price_data = price_data[
            (price_data["date"].dt.date >= config.start_date)
            & (price_data["date"].dt.date <= config.end_date)
        ]
        calendar = pd.DatetimeIndex(price_data["date"].drop_duplicates().sort_values())
        if calendar.empty:
            raise ValueError("指定区间没有可用交易日行情")

        daily_columns = [
            column for column in price_columns if column not in {"date", "symbol"}
        ]
        price_map = {}
        for trading_date, frame in price_data.groupby("date", sort=False):
            daily_frame = frame.set_index("symbol").loc[:, daily_columns]
            if daily_frame.index.duplicated().any():
                raise ValueError(f"行情存在重复的 date/symbol: {trading_date.date()}")
            price_map[trading_date] = daily_frame
        return PreparedMarketData(
            price_data=price_data,
            calendar=calendar,
            price_map=price_map,
        )

    def run(
        self,
        prices: pd.DataFrame,
        targets: pd.DataFrame,
        benchmark_prices: pd.DataFrame | None = None,
        *,
        prepared_market_data: PreparedMarketData | None = None,
        confirmed_delisting_dates: Mapping[str, object] | None = None,
    ) -> BacktestResult:
        market_data = prepared_market_data or self.prepare_market_data(
            prices, self.config
        )
        calendar = market_data.calendar
        price_map = market_data.price_map
        normalized_delisting_dates = self._normalize_confirmed_delisting_dates(
            confirmed_delisting_dates
        )
        # 将 T 日收盘后的信号映射至下一个实际交易日，禁止同日成交。
        execution_plans = self._build_execution_plans(targets, calendar)
        positions: dict[str, float] = {}
        last_close_prices: dict[str, float] = {}
        cash = self.config.initial_capital
        nav_rows: list[dict] = []
        trades: list[dict] = []

        for trading_date in calendar:
            today_prices = price_map[trading_date]
            open_values, blocked_symbols = self._mark_positions_to_open(
                positions, last_close_prices, today_prices
            )
            if trading_date in execution_plans:
                signal_date, planned_weights = execution_plans[trading_date]
                if blocked_symbols:
                    # 无有效开盘价的持仓无法以可验证价格卖出，整次调仓保持原组合。
                    trades.append(
                        {
                            "date": trading_date,
                            "signal_date": signal_date,
                            "symbol": ",".join(sorted(blocked_symbols)),
                            "side": "SKIP_REBALANCE",
                            "raw_open": None,
                            "adjusted_open": None,
                            "notional": 0.0,
                            "cost": 0.0,
                            "reason": "held_symbol_missing_open",
                        }
                    )
                else:
                    positions, cash, rebalance_trades = self._rebalance(
                        trading_date,
                        signal_date,
                        open_values,
                        cash,
                        planned_weights,
                        today_prices,
                    )
                    trades.extend(rebalance_trades)
            else:
                positions = open_values

            positions, last_close_prices = self._mark_positions_to_close(
                positions, last_close_prices, today_prices
            )
            # 行情终结 (退市/摘牌) 清算: 最后交易日收盘后按收盘价强制变现。
            positions, cash, last_close_prices, delist_trades = (
                self._liquidate_confirmed_delisted_positions(
                    trading_date,
                    positions,
                    cash,
                    last_close_prices,
                    today_prices,
                    normalized_delisting_dates,
                )
            )
            trades.extend(delist_trades)
            nav = cash + sum(positions.values())
            nav_rows.append(
                {
                    "date": trading_date,
                    "nav": nav,
                    "cash": cash,
                    "positions_value": sum(positions.values()),
                }
            )

        daily_nav = pd.DataFrame(nav_rows)
        daily_nav["benchmark_nav"] = self._calculate_benchmark_nav(
            daily_nav, benchmark_prices
        )
        trade_columns = [
            "date",
            "signal_date",
            "symbol",
            "side",
            "raw_open",
            "adjusted_open",
            "notional",
            "cost",
            "reason",
        ]
        return BacktestResult(
            daily_nav=daily_nav, trades=pd.DataFrame(trades, columns=trade_columns)
        )

    def _build_execution_plans(
        self, targets: pd.DataFrame, calendar: pd.DatetimeIndex
    ) -> dict:
        if targets.empty:
            return {}
        target_data = targets.copy()
        target_data["date"] = pd.to_datetime(target_data["date"])
        plans = {}
        for signal_date, group in target_data.groupby("date"):
            following_dates = calendar[calendar > signal_date]
            if following_dates.empty:
                continue
            plans[following_dates[0]] = (
                signal_date,
                {
                    row.symbol: float(row.target_weight)
                    for row in group.sort_values("symbol").itertuples(index=False)
                },
            )
        return plans

    def _mark_positions_to_open(self, positions, last_close_prices, today_prices):
        open_values = {}
        blocked_symbols = set()
        for symbol, value in positions.items():
            row = self._get_price_row(today_prices, symbol)
            open_price = row.get("open_hfq") if row is not None else None
            previous_close = last_close_prices.get(symbol)
            if pd.isna(open_price) or not open_price or not previous_close:
                open_values[symbol] = value
                blocked_symbols.add(symbol)
            else:
                # 持仓保存为上一收盘的后复权价值，先滚动到开盘后再进行调仓。
                open_values[symbol] = value * float(open_price) / previous_close
        return open_values, blocked_symbols

    def _rebalance(
        self,
        trading_date,
        signal_date,
        open_values,
        cash,
        planned_weights,
        today_prices,
    ):
        available_weights = {}
        for symbol, weight in planned_weights.items():
            row = self._get_price_row(today_prices, symbol)
            if row is not None and pd.notna(row.get("open_hfq")):
                available_weights[symbol] = weight
        before_nav = cash + sum(open_values.values())
        symbols = sorted(set(open_values) | set(available_weights))
        notional_by_symbol = {
            symbol: abs(
                available_weights.get(symbol, 0.0) * before_nav
                - open_values.get(symbol, 0.0)
            )
            for symbol in symbols
        }
        total_cost = (
            sum(notional_by_symbol.values()) * self.config.transaction_cost_rate
        )
        after_cost_nav = before_nav - total_cost
        # 以扣成本后的净值分配目标权重，避免手续费把现金余额推成负数。
        positions = {
            symbol: weight * after_cost_nav
            for symbol, weight in available_weights.items()
            if weight > 0
        }
        cash = after_cost_nav * (1 - sum(available_weights.values()))
        trades = []
        for symbol, notional in notional_by_symbol.items():
            if notional == 0:
                continue
            target_value = available_weights.get(symbol, 0.0) * before_nav
            current_value = open_values.get(symbol, 0.0)
            row = self._get_price_row(today_prices, symbol)
            trades.append(
                {
                    "date": trading_date,
                    "signal_date": signal_date,
                    "symbol": symbol,
                    "side": "BUY" if target_value > current_value else "SELL",
                    "raw_open": row.get("open"),
                    "adjusted_open": row.get("open_hfq"),
                    "notional": notional,
                    "cost": notional * self.config.transaction_cost_rate,
                    "reason": self.config.rebalance_trade_reason,
                }
            )
        return positions, cash, trades

    @staticmethod
    def _mark_positions_to_close(positions, last_close_prices, today_prices):
        close_values = {}
        for symbol, value in positions.items():
            row = DailyBacktestEngine._get_price_row(today_prices, symbol)
            open_price = row.get("open_hfq") if row is not None else None
            close_price = row.get("close_hfq") if row is not None else None
            if (
                pd.notna(open_price)
                and open_price
                and pd.notna(close_price)
                and close_price
            ):
                # 只计算开盘至收盘的后复权变动；隔夜变动已在 _mark_positions_to_open 计入。
                close_values[symbol] = value * float(close_price) / float(open_price)
                last_close_prices[symbol] = float(close_price)
            else:
                close_values[symbol] = value
        return close_values, last_close_prices

    @staticmethod
    def _liquidate_confirmed_delisted_positions(
        trading_date,
        positions,
        cash,
        last_close_prices,
        today_prices,
        confirmed_delisting_dates,
    ):
        """确认退市日收盘后按收盘价强制清算持仓。

        清算后持仓移出组合, 避免次日缺失行情触发 blocked 冻结整次调仓,
        也避免持仓价值悬空在最后价格造成净值失真。
        """
        delist_trades = []
        for symbol in list(positions):
            if confirmed_delisting_dates.get(symbol) != trading_date:
                continue
            row = DailyBacktestEngine._get_price_row(today_prices, symbol)
            if row is None or pd.isna(row.get("close_hfq")) or not row.get("close_hfq"):
                continue
            value = positions.pop(symbol)
            previous_close = last_close_prices.get(symbol)
            if previous_close and not pd.isna(previous_close):
                # 收盘标记可能因缺失开盘价未滚动, 此处按收盘价补齐最后一日涨跌。
                value = value * float(row["close_hfq"]) / previous_close
            cash += value
            last_close_prices.pop(symbol, None)
            delist_trades.append(
                {
                    "date": trading_date,
                    "signal_date": None,
                    "symbol": symbol,
                    "side": "DELIST",
                    "raw_open": row.get("raw_close", row.get("close")),
                    "adjusted_open": row.get("close_hfq"),
                    "notional": value,
                    "cost": 0.0,
                    "reason": "delisted_liquidation",
                }
            )
        return positions, cash, last_close_prices, delist_trades

    @staticmethod
    def _normalize_confirmed_delisting_dates(
        confirmed_delisting_dates: Mapping[str, object] | None,
    ) -> dict[str, pd.Timestamp]:
        normalized_dates = {}
        for symbol, raw_date in (confirmed_delisting_dates or {}).items():
            parsed_date = pd.to_datetime(raw_date, errors="coerce")
            if pd.isna(parsed_date):
                raise ValueError(f"确认退市日期无效: {symbol}={raw_date}")
            normalized_dates[str(symbol)] = pd.Timestamp(parsed_date).normalize()
        return normalized_dates

    @staticmethod
    def _get_price_row(today_prices, symbol):
        """Return one symbol's price row from either compact or legacy price maps."""

        if isinstance(today_prices, pd.DataFrame):
            if symbol not in today_prices.index:
                return None
            row = today_prices.loc[symbol]
            if isinstance(row, pd.DataFrame):
                return row.iloc[-1]
            return row
        return today_prices.get(symbol)

    def _calculate_benchmark_nav(self, daily_nav, benchmark_prices):
        if benchmark_prices is None or benchmark_prices.empty:
            return pd.Series(index=daily_nav.index, dtype="float64")
        benchmark = benchmark_prices.copy()
        benchmark["date"] = pd.to_datetime(benchmark["date"])
        benchmark = benchmark.dropna(subset=["close_hfq"]).drop_duplicates(
            "date", keep="last"
        )
        benchmark = benchmark.set_index("date")["close_hfq"].reindex(
            daily_nav["date"], method="ffill"
        )
        first_valid = benchmark.first_valid_index()
        if first_valid is None:
            return pd.Series(index=daily_nav.index, dtype="float64")
        # 基准以首个可用后复权收盘价归一化，便于与策略初始资金直接比较。
        return benchmark / benchmark.loc[first_valid] * self.config.initial_capital
