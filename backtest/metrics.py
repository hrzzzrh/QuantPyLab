import math

import pandas as pd


def calculate_performance_metrics(daily_nav: pd.DataFrame, column: str = "nav") -> dict[str, float | int | None]:
    series = daily_nav.set_index("date")[column].dropna()
    if series.empty:
        return {
            "total_return": None,
            "annualized_return": None,
            "annualized_volatility": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "trading_days": 0,
        }

    total_return = series.iloc[-1] / series.iloc[0] - 1
    elapsed_days = max((series.index[-1] - series.index[0]).days, 1)
    annualized_return = (1 + total_return) ** (365.25 / elapsed_days) - 1
    daily_returns = series.pct_change().dropna()
    annualized_volatility = daily_returns.std(ddof=0) * math.sqrt(252) if not daily_returns.empty else 0.0
    sharpe_ratio = (
        daily_returns.mean() / daily_returns.std(ddof=0) * math.sqrt(252)
        if len(daily_returns) > 1 and daily_returns.std(ddof=0) > 0
        else None
    )
    drawdown = series / series.cummax() - 1

    return {
        "total_return": float(total_return),
        "annualized_return": float(annualized_return),
        "annualized_volatility": float(annualized_volatility),
        "sharpe_ratio": float(sharpe_ratio) if sharpe_ratio is not None else None,
        "max_drawdown": float(drawdown.min()),
        "trading_days": int(len(series)),
    }
