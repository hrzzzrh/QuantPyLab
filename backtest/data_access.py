import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import pandas as pd

from analysis.factors.registry import get_factor_definition
from backtest.config import BacktestConfig
from storage.database.manager import DBManager


@dataclass(frozen=True)
class IndicatorField:
    source_name: str
    alias: str


class BacktestDataAccess:
    """通过统一视图加载回测数据，并统一处理点时财务指标。"""

    _KLINE_SIGNAL_COLUMNS = frozenset({"high", "low", "volume", "amount"})
    _VALUATION_SIGNAL_COLUMNS = frozenset({"ps_ttm", "pcf_ttm", "market_cap"})

    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

    def load_market_data(
        self,
        config: BacktestConfig,
        lookback_days: int,
        indicator_fields: tuple[IndicatorField, ...] = (),
        kline_fields: tuple[str, ...] = (),
        data_end_date: date | None = None,
        valuation_fields: tuple[str, ...] = (),
    ) -> pd.DataFrame:
        effective_end_date = data_end_date or config.end_date
        if effective_end_date < config.end_date:
            raise ValueError("data_end_date 不能早于回测结束日期")
        unknown_kline_fields = set(kline_fields) - self._KLINE_SIGNAL_COLUMNS
        if unknown_kline_fields:
            raise ValueError(
                "不支持的行情信号字段: " + ", ".join(sorted(unknown_kline_fields))
            )
        valuation_projection = self._build_valuation_projection(valuation_fields)
        view_names = ["v_daily_valuation", "daily_kline"]
        if indicator_fields:
            view_names.append("fin_indicator")
        self.db_manager.ensure_views(*view_names)
        conn = self.db_manager.get_duckdb_conn()
        lookback_start = self._get_lookback_start(
            conn, config.start_date, lookback_days
        )
        indicator_sql = self._build_indicator_join(indicator_fields)
        kline_projection = self._build_kline_projection(kline_fields)
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
                    {valuation_projection},
                    kline.open
                    {kline_projection}
                FROM v_daily_valuation AS valuation
                INNER JOIN daily_kline AS kline
                    ON valuation.symbol = kline.symbol AND valuation.date = CAST(kline.date AS DATE)
                WHERE valuation.date BETWEEN ? AND ?
            ) AS daily_data
            {self._build_indicator_asof_join(indicator_fields)}
            ORDER BY daily_data.date, daily_data.symbol
            """,
            [lookback_start, effective_end_date],
        ).df()
        frame["date"] = pd.to_datetime(frame["date"])
        # 后复权开盘价让开盘成交与收盘收益使用同一经济口径。
        frame["open_hfq"] = frame["open"] * frame["close_hfq"] / frame["raw_close"]
        return frame

    def load_factor_data(
        self,
        config: BacktestConfig,
        factor_names: tuple[str, ...],
        factor_parameters: Mapping[str, Mapping[str, object]] | None = None,
        minimum_history_days: int = 0,
        data_end_date: date | None = None,
        include_market_cap: bool = False,
    ) -> pd.DataFrame:
        """按注册因子需求加载点时行情和财务输入，不计算因子值。"""
        names = tuple(dict.fromkeys(factor_names))
        if not names:
            raise ValueError("至少需要指定一个因子")
        if (
            isinstance(minimum_history_days, bool)
            or not isinstance(minimum_history_days, int)
            or minimum_history_days < 0
        ):
            raise ValueError("minimum_history_days 必须是非负整数")

        indicator_fields: dict[str, IndicatorField] = {}
        kline_fields: set[str] = set()
        valuation_fields: set[str] = set()
        if include_market_cap:
            valuation_fields.add("market_cap")
        lookback_days = minimum_history_days
        parameter_map = factor_parameters or {}
        if not isinstance(parameter_map, Mapping):
            raise ValueError("factor_parameters 必须是映射")
        for factor_name in names:
            definition = get_factor_definition(factor_name)
            factor_parameters_for_name = parameter_map.get(factor_name, {})
            if not isinstance(factor_parameters_for_name, Mapping):
                raise ValueError(f"因子 {factor_name} 的参数必须是映射")
            lookback_days = max(
                lookback_days,
                definition.get_lookback_days(factor_parameters_for_name),
            )
            metadata = definition.metadata
            for field in metadata.inputs:
                if field.source == "indicator":
                    indicator_fields[field.alias] = IndicatorField(
                        field.source_name, field.alias
                    )
                elif field.source == "kline":
                    kline_fields.add(field.alias)
                elif field.source == "valuation":
                    if field.alias not in {"close_hfq", "pe_ttm", "pb"}:
                        valuation_fields.add(field.alias)

        load_arguments = (
            config,
            lookback_days,
            tuple(indicator_fields.values()),
            tuple(sorted(kline_fields)),
        )
        if data_end_date is None:
            return self.load_market_data(
                *load_arguments, valuation_fields=tuple(sorted(valuation_fields))
            )
        return self.load_market_data(
            *load_arguments,
            data_end_date=data_end_date,
            valuation_fields=tuple(sorted(valuation_fields)),
        )

    def load_point_in_time_industry(self, points: pd.DataFrame) -> pd.DataFrame:
        """按信号日从历史行业视图加载最近生效的申万行业代码。"""

        required = {"date", "symbol"}
        missing = required - set(points.columns)
        if missing:
            raise ValueError("行业点时查询缺少字段: " + ", ".join(sorted(missing)))
        normalized = points.loc[:, ["date", "symbol"]].copy()
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
        normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
        if (
            normalized["date"].isna().any()
            or normalized["symbol"].isna().any()
            or normalized["symbol"].eq("").any()
        ):
            raise ValueError("行业点时查询的 date 和 symbol 不能为空")
        if normalized.duplicated(["date", "symbol"]).any():
            raise ValueError("行业点时查询不能包含重复的 date/symbol")

        self.db_manager.ensure_views("industry_classification_sw")
        conn = self.db_manager.get_duckdb_conn()
        relation_name = "_industry_point_in_time_points"
        conn.register(relation_name, normalized)
        try:
            frame = conn.execute(
                f"""
                SELECT points.date, points.symbol, industry.industry_code
                FROM (
                    SELECT CAST(date AS DATE) AS date, symbol
                    FROM {relation_name}
                    ORDER BY symbol, date
                ) AS points
                ASOF LEFT JOIN (
                    SELECT symbol, effective_date, industry_code
                    FROM industry_classification_sw
                    ORDER BY symbol, effective_date
                ) AS industry
                  ON points.symbol = industry.symbol
                 AND points.date >= industry.effective_date
                ORDER BY points.date, points.symbol
                """
            ).df()
        finally:
            conn.unregister(relation_name)
        frame["date"] = pd.to_datetime(frame["date"])
        frame["symbol"] = frame["symbol"].astype("string")
        return frame

    def get_industry_snapshot_metadata(self) -> dict[str, object]:
        """返回当前历史行业 canonical 快照的可复现摘要。"""

        self.db_manager.ensure_views("industry_classification_sw")
        conn = self.db_manager.get_duckdb_conn()
        snapshot = conn.execute(
            """
            SELECT symbol, effective_date, industry_code, source_updated_date
            FROM industry_classification_sw
            ORDER BY symbol, effective_date, industry_code, source_updated_date
            """
        ).df()
        canonical = snapshot.copy()
        for column in ("effective_date", "source_updated_date"):
            canonical[column] = pd.to_datetime(
                canonical[column], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        payload = canonical.to_csv(index=False, header=False, lineterminator="\n")
        source_dates = pd.to_datetime(
            snapshot["source_updated_date"], errors="coerce"
        ).dropna()
        return {
            "row_count": int(len(snapshot)),
            "symbol_count": int(snapshot["symbol"].nunique()),
            "snapshot_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            "source_updated_date_min": (
                source_dates.min().date().isoformat()
                if not source_dates.empty
                else None
            ),
            "source_updated_date_max": (
                source_dates.max().date().isoformat()
                if not source_dates.empty
                else None
            ),
        }

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
        if lookback_days <= 0:
            return start_date
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
    def _build_kline_projection(kline_fields: tuple[str, ...]) -> str:
        if not kline_fields:
            return ""
        return ", " + ", ".join(
            f'kline."{field}" AS "{field}"' for field in kline_fields
        )

    @classmethod
    def _build_valuation_projection(cls, valuation_fields: tuple[str, ...]) -> str:
        unknown_fields = set(valuation_fields) - cls._VALUATION_SIGNAL_COLUMNS
        if unknown_fields:
            raise ValueError(
                "不支持的估值信号字段: " + ", ".join(sorted(unknown_fields))
            )
        columns = [
            "valuation.pe_ttm",
            "valuation.pb",
            *(f"valuation.{field} AS {field}" for field in valuation_fields),
        ]
        return ",\n                    ".join(columns)

    @staticmethod
    def _build_indicator_asof_join(indicator_fields: tuple[IndicatorField, ...]) -> str:
        if not indicator_fields:
            return ""
        # 指标只有在公告日当天及之后可见，不能按报告期直接连接。
        return """
            ASOF LEFT JOIN deduplicated_indicators AS indicators
                ON daily_data.symbol = indicators.symbol AND daily_data.date >= indicators.pub_date
        """
