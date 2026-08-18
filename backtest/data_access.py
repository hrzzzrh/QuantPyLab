from dataclasses import dataclass

import pandas as pd

from backtest.config import BacktestConfig
from storage.database.manager import DBManager


@dataclass(frozen=True)
class IndicatorField:
    source_name: str
    alias: str


class BacktestDataAccess:
    """通过统一视图加载回测数据，并统一处理点时财务指标。"""

    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

    def load_market_data(
        self,
        config: BacktestConfig,
        lookback_days: int,
        indicator_fields: tuple[IndicatorField, ...] = (),
    ) -> pd.DataFrame:
        view_names = ["v_daily_valuation", "daily_kline"]
        if indicator_fields:
            view_names.append("fin_indicator")
        self.db_manager.ensure_views(*view_names)
        conn = self.db_manager.get_duckdb_conn()
        lookback_start = self._get_lookback_start(
            conn, config.start_date, lookback_days
        )
        indicator_sql = self._build_indicator_join(indicator_fields)
        frame = conn.execute(
            f"""
            {indicator_sql}
            SELECT daily_data.*{self._build_indicator_projection(indicator_fields)}
            FROM (
                SELECT
                    valuation.date,
                    valuation.symbol,
                    valuation.raw_close,
                    valuation.close_hfq,
                    valuation.pe_ttm,
                    valuation.pb,
                    kline.open
                FROM v_daily_valuation AS valuation
                INNER JOIN daily_kline AS kline
                    ON valuation.symbol = kline.symbol AND valuation.date = CAST(kline.date AS DATE)
                WHERE valuation.date BETWEEN ? AND ?
            ) AS daily_data
            {self._build_indicator_asof_join(indicator_fields)}
            ORDER BY daily_data.date, daily_data.symbol
            """,
            [lookback_start, config.end_date],
        ).df()
        frame["date"] = pd.to_datetime(frame["date"])
        # 后复权开盘价让开盘成交与收盘收益使用同一经济口径。
        frame["open_hfq"] = frame["open"] * frame["close_hfq"] / frame["raw_close"]
        return frame

    def load_benchmark_prices(self, config: BacktestConfig) -> pd.DataFrame:
        if not config.benchmark_symbol:
            return pd.DataFrame(columns=["date", "close_hfq"])
        self.db_manager.ensure_views("etf_kline")
        frame = (
            self.db_manager.get_duckdb_conn()
            .execute(
                """
            SELECT CAST(date AS DATE) AS date, close * adj_factor AS close_hfq
            FROM etf_kline
            WHERE symbol = ? AND CAST(date AS DATE) BETWEEN ? AND ?
            ORDER BY date
            """,
                [config.benchmark_symbol, config.start_date, config.end_date],
            )
            .df()
        )
        frame["date"] = pd.to_datetime(frame["date"])
        return frame

    @staticmethod
    def _get_lookback_start(conn, start_date, lookback_days: int):
        row = conn.execute(
            """
            SELECT MIN(date)
            FROM (
                SELECT CAST(date AS DATE) AS date
                FROM daily_kline
                WHERE CAST(date AS DATE) < ?
                GROUP BY date
                ORDER BY date DESC
                LIMIT ?
            )
            """,
            [start_date, lookback_days],
        ).fetchone()
        if row is None or row[0] is None:
            raise ValueError(f"回测开始日前不足 {lookback_days} 个交易日")
        return row[0]

    @staticmethod
    def _build_indicator_join(indicator_fields: tuple[IndicatorField, ...]) -> str:
        if not indicator_fields:
            return ""
        columns = ",\n                ".join(
            f'"{field.source_name}" AS {field.alias}' for field in indicator_fields
        )
        tie_breaker_parts = [
            "COALESCE(CAST(report_date AS VARCHAR), '<NULL>')",
            *[
                f"COALESCE(CAST(\"{field.source_name}\" AS VARCHAR), '<NULL>')"
                for field in indicator_fields
            ],
        ]
        tie_breaker = "md5(concat_ws('|', " + ", ".join(tie_breaker_parts) + "))"
        return f"""
            WITH indicator_history AS (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 8), '%Y%m%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 8), '%Y%m%d')::DATE
                    ) AS pub_date,
                    {columns},
                    report_date,
                    {tie_breaker} AS record_tie_breaker
                FROM fin_indicator
            ),
            deduplicated_indicators AS (
                SELECT *
                FROM indicator_history
                WHERE pub_date IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol, pub_date
                    ORDER BY report_date DESC, record_tie_breaker DESC
                ) = 1
            )
        """

    @staticmethod
    def _build_indicator_projection(
        indicator_fields: tuple[IndicatorField, ...],
    ) -> str:
        return (
            ""
            if not indicator_fields
            else ", "
            + ", ".join(f"indicators.{field.alias}" for field in indicator_fields)
        )

    @staticmethod
    def _build_indicator_asof_join(indicator_fields: tuple[IndicatorField, ...]) -> str:
        if not indicator_fields:
            return ""
        # 指标只有在公告日当天及之后可见，不能按报告期直接连接。
        return """
            ASOF LEFT JOIN deduplicated_indicators AS indicators
                ON daily_data.symbol = indicators.symbol AND daily_data.date >= indicators.pub_date
        """
