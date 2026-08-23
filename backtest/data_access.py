import gc
import hashlib
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date

import numpy as np
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

    # Keep the query allocator below the process-level research budget. Large
    # result frames are materialized in bounded batches and detached before
    # the next query, so a smaller DuckDB limit avoids stacking allocator
    # blocks with Pandas' copies.
    _DUCKDB_MEMORY_LIMIT = "256MB"
    _DUCKDB_THREADS = 2
    _KLINE_SYMBOL_BATCH_SIZE = 250
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
        financial_signal_dates_only: bool = False,
    ) -> pd.DataFrame:
        """在连接级锁内加载一份自洽的行情与点时财务数据。"""

        try:
            with self._duckdb_guard():
                return self._load_market_data(
                    config,
                    lookback_days,
                    indicator_fields,
                    kline_fields,
                    data_end_date,
                    valuation_fields,
                    financial_signal_dates_only,
                )
        finally:
            # A market-data frame is fully detached before this method returns.
            # Closing the transient in-memory DuckDB connection releases its
            # allocator blocks between large factor loads; the next call
            # recreates only the views it needs.
            close_duckdb = getattr(self.db_manager, "close_duckdb", None)
            if callable(close_duckdb):
                close_duckdb()

    @contextmanager
    def _duckdb_guard(self):
        lock = getattr(self.db_manager, "duckdb_lock", None)
        if lock is None:
            yield
            return
        with lock:
            yield

    def _load_market_data(
        self,
        config: BacktestConfig,
        lookback_days: int,
        indicator_fields: tuple[IndicatorField, ...] = (),
        kline_fields: tuple[str, ...] = (),
        data_end_date: date | None = None,
        valuation_fields: tuple[str, ...] = (),
        financial_signal_dates_only: bool = False,
    ) -> pd.DataFrame:
        effective_end_date = data_end_date or config.end_date
        if effective_end_date < config.end_date:
            raise ValueError("data_end_date 不能早于回测结束日期")
        unknown_kline_fields = set(kline_fields) - self._KLINE_SIGNAL_COLUMNS
        if unknown_kline_fields:
            raise ValueError(
                "不支持的行情信号字段: " + ", ".join(sorted(unknown_kline_fields))
            )
        valuation_fields = tuple(dict.fromkeys(valuation_fields))
        self._build_valuation_projection(valuation_fields)
        view_names = [
            "daily_kline_raw",
            "daily_kline_calendar",
            "share_capital",
            "fin_ttm",
            "fin_balance_sheet",
        ]
        if indicator_fields:
            view_names.append("fin_indicator")
        self.db_manager.ensure_views(*view_names)
        conn = self.db_manager.get_duckdb_conn()
        previous_query_settings = self._get_query_connection_settings(conn)
        self._configure_query_connection(conn)
        symbol_relation = "_backtest_requested_symbols"
        relation_registered = False
        batch_symbol_relation = "_backtest_kline_symbols"
        batch_relation_registered = False
        try:
            lookback_start = self._get_lookback_start(
                conn, config.start_date, lookback_days, "daily_kline_calendar"
            )
            requested_symbols = conn.execute(
                """
                SELECT DISTINCT symbol
                FROM daily_kline_calendar
                WHERE date BETWEEN ? AND ?
                ORDER BY symbol
                """,
                [lookback_start, effective_end_date],
            ).df()
            requested_symbols = requested_symbols.loc[:, ["symbol"]].copy()
            requested_symbols = requested_symbols.reset_index(drop=True)
            requested_symbols["symbol"] = requested_symbols["symbol"].astype("string")
            conn.register(symbol_relation, requested_symbols)
            relation_registered = True
            # 先物化小规模财务历史，再读取大行情表；避免在千万行行情驻留时
            # 继续从 DuckDB 财务视图读取 Parquet，导致大执行计划下的结果不稳定。
            valuation_histories = (
                self._load_capital_history(conn, effective_end_date, symbol_relation),
                self._load_ttm_history(conn, effective_end_date, symbol_relation),
                self._load_assets_history(conn, effective_end_date, symbol_relation),
            )
            indicator_history = None
            if indicator_fields:
                indicator_history = self._load_indicator_history(
                    conn,
                    indicator_fields,
                    effective_end_date,
                    symbol_relation,
                )
            kline_projection = self._build_kline_projection(kline_fields)
            requested_symbol_values = requested_symbols["symbol"].tolist()
            kline_columns = [
                "date",
                "symbol",
                "raw_close",
                "adj_factor",
                "open",
                *kline_fields,
            ]
            numeric_kline_columns = [
                column for column in kline_columns if column not in {"date", "symbol"}
            ]
            count_row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM daily_kline_raw AS kline
                WHERE CAST(kline.date AS DATE) BETWEEN ? AND ?
                  AND kline.symbol IN (SELECT symbol FROM {symbol_relation})
                """,
                [lookback_start, effective_end_date],
            ).fetchone()
            expected_kline_rows = int(count_row[0] or 0)
            column_arrays = {
                "date": np.empty(expected_kline_rows, dtype="datetime64[ns]"),
                "symbol": np.empty(expected_kline_rows, dtype="int32"),
                **{
                    column: np.empty(expected_kline_rows, dtype="float64")
                    for column in numeric_kline_columns
                },
            }
            write_offset = 0
            for offset in range(
                0,
                len(requested_symbol_values),
                self._KLINE_SYMBOL_BATCH_SIZE,
            ):
                batch_symbols = pd.DataFrame(
                    {
                        "symbol": requested_symbol_values[
                            offset : offset + self._KLINE_SYMBOL_BATCH_SIZE
                        ]
                    }
                )
                conn.register(batch_symbol_relation, batch_symbols)
                batch_relation_registered = True
                try:
                    batch_frame = self._fetch_dataframe(
                        conn,
                        f"""
                        SELECT
                            CAST(kline.date AS DATE) AS date,
                            kline.symbol,
                            kline.close AS raw_close,
                            kline.adj_factor,
                            kline.open
                            {kline_projection}
                        FROM daily_kline_raw AS kline
                        WHERE CAST(kline.date AS DATE) BETWEEN ? AND ?
                          AND kline.symbol IN (
                              SELECT symbol FROM {batch_symbol_relation}
                          )
                        """,
                        [lookback_start, effective_end_date],
                    )
                    if batch_frame.duplicated(["date", "symbol"]).any():
                        del batch_frame
                        gc.collect()
                        batch_frame = self._fetch_dataframe(
                            conn,
                            f"""
                            SELECT
                                CAST(kline.date AS DATE) AS date,
                                kline.symbol,
                                kline.close AS raw_close,
                                kline.adj_factor,
                                kline.open
                                {kline_projection},
                                kline.open AS _kline_open,
                                kline.high AS _kline_high,
                                kline.low AS _kline_low,
                                kline.close AS _kline_close,
                                kline.volume AS _kline_volume,
                                kline.amount AS _kline_amount,
                                kline.adj_factor AS _kline_adj_factor,
                                kline.filename AS _kline_filename
                            FROM daily_kline_raw AS kline
                            WHERE CAST(kline.date AS DATE) BETWEEN ? AND ?
                              AND kline.symbol IN (
                                  SELECT symbol FROM {batch_symbol_relation}
                              )
                            """,
                            [lookback_start, effective_end_date],
                        )
                        batch_frame = self._canonicalize_kline_rows(batch_frame)
                    batch_frame["date"] = pd.to_datetime(batch_frame["date"])
                    row_count = len(batch_frame)
                    end_offset = write_offset + row_count
                    column_arrays["date"][write_offset:end_offset] = batch_frame[
                        "date"
                    ].to_numpy(dtype="datetime64[ns]")
                    column_arrays["symbol"][write_offset:end_offset] = pd.Categorical(
                        batch_frame["symbol"], categories=requested_symbol_values
                    ).codes
                    for column in numeric_kline_columns:
                        column_arrays[column][write_offset:end_offset] = pd.to_numeric(
                            batch_frame[column], errors="coerce"
                        ).to_numpy(dtype="float64")
                    write_offset = end_offset
                    del batch_frame
                finally:
                    conn.unregister(batch_symbol_relation)
                    batch_relation_registered = False
            kline_frame = pd.DataFrame(
                {
                    "date": column_arrays["date"][:write_offset],
                    "symbol": pd.Categorical.from_codes(
                        column_arrays["symbol"][:write_offset],
                        categories=requested_symbol_values,
                    ),
                    **{
                        column: column_arrays[column][:write_offset]
                        for column in numeric_kline_columns
                    },
                },
                copy=False,
            )
            del column_arrays
            # The preallocated arrays avoid retaining all batch frames while
            # pandas creates a second full-size object during concat.
            # 估值和行情分开物化；不把财务历史区间连接放进大型日行情查询计划。
            # 这样避免 DuckDB 在不同视图加载顺序下错误复用区间连接结果。
            if financial_signal_dates_only:
                # 滚动行情因子需要完整日频 close_hfq，但估值和财务指标只会在
                # 月末信号日参与选股。若把历史财务值复制到每个交易日，会在
                # 数百万行行情上重复构造宽表并显著抬高峰值内存。
                kline_frame["date"] = pd.to_datetime(kline_frame["date"])
                kline_frame["raw_close"] = pd.to_numeric(
                    kline_frame["raw_close"], errors="coerce"
                )
                kline_frame["adj_factor"] = pd.to_numeric(
                    kline_frame["adj_factor"], errors="coerce"
                )
                kline_frame["close_hfq"] = (
                    kline_frame["raw_close"] * kline_frame["adj_factor"]
                )
                periods = kline_frame["date"].dt.to_period("M")
                month_end_dates = kline_frame.groupby(periods, sort=False)[
                    "date"
                ].transform("max")
                financial_signal_mask = kline_frame["date"].eq(month_end_dates) & (
                    kline_frame["date"].dt.date >= config.start_date
                )
                financial_input = kline_frame.loc[financial_signal_mask].copy()
                financial_input["_financial_row_id"] = financial_input.index.to_numpy()
                financial_frame = self._build_valuation_frame(
                    conn,
                    financial_input,
                    effective_end_date,
                    valuation_fields,
                    symbol_relation,
                    valuation_histories=valuation_histories,
                    drop_missing_rows=False,
                )
                financial_frame["date"] = pd.to_datetime(financial_frame["date"])
                if indicator_history is not None:
                    financial_frame = self._merge_indicator_point_in_time(
                        financial_frame,
                        indicator_history,
                        indicator_fields,
                        copy_market_data=False,
                        history_is_canonical=True,
                        market_data_is_sorted=True,
                    )
                    financial_frame.drop(columns=["_指标_effective_date"], inplace=True)
                financial_columns = [
                    "pe_ttm",
                    "pb",
                    *valuation_fields,
                    *(field.alias for field in indicator_fields),
                ]
                financial_row_ids = financial_frame["_financial_row_id"].to_numpy(
                    dtype="intp"
                )
                for column in financial_columns:
                    values = pd.to_numeric(
                        financial_frame[column], errors="coerce"
                    ).to_numpy(dtype="float64")
                    kline_frame[column] = np.nan
                    kline_frame.loc[financial_row_ids, column] = values
                frame = kline_frame
                del financial_frame
            else:
                frame = self._build_valuation_frame(
                    conn,
                    kline_frame,
                    effective_end_date,
                    valuation_fields,
                    symbol_relation,
                    valuation_histories=valuation_histories,
                )
                del kline_frame
                frame["date"] = pd.to_datetime(frame["date"])
                if indicator_history is not None:
                    frame = self._merge_indicator_point_in_time(
                        frame,
                        indicator_history,
                        indicator_fields,
                        copy_market_data=False,
                        history_is_canonical=True,
                        market_data_is_sorted=True,
                    )
                    frame.drop(columns=["_指标_effective_date"], inplace=True)
            del valuation_histories, indicator_history
        finally:
            try:
                if batch_relation_registered:
                    conn.unregister(batch_symbol_relation)
            finally:
                try:
                    if relation_registered:
                        conn.unregister(symbol_relation)
                finally:
                    self._restore_query_connection(conn, previous_query_settings)
        # 后复权开盘价让开盘成交与收盘收益使用同一经济口径。
        frame["open_hfq"] = frame["open"] * frame["close_hfq"] / frame["raw_close"]
        if financial_signal_dates_only:
            # Monthly factor consumers sort their per-symbol history before
            # rolling calculations and the execution engine builds its own
            # date calendar. Avoid a second full-frame date/symbol sort here;
            # it otherwise duplicates millions of rows at the peak.
            frame.reset_index(drop=True, inplace=True)
        else:
            frame = frame.sort_values(["date", "symbol"], kind="mergesort").reset_index(
                drop=True
            )
        frame["symbol"] = frame["symbol"].astype("category")
        if financial_signal_dates_only:
            output_columns = [
                "date",
                "symbol",
                "raw_close",
                "close_hfq",
                "pe_ttm",
                "pb",
                *valuation_fields,
                *(field.alias for field in indicator_fields),
                "open",
                *kline_fields,
                "open_hfq",
            ]
            output_columns = list(dict.fromkeys(output_columns))
            missing_output_columns = set(output_columns) - set(frame.columns)
            if missing_output_columns:
                raise ValueError(
                    "行情结果缺少字段: " + ", ".join(sorted(missing_output_columns))
                )
            frame.drop(
                columns=[
                    column for column in frame.columns if column not in output_columns
                ],
                inplace=True,
            )
        return frame

    @classmethod
    def _configure_query_connection(cls, conn) -> None:
        """为大回测查询设置有界内存和线程数，避免挤占整个宿主机。"""

        conn.execute(f"SET memory_limit='{cls._DUCKDB_MEMORY_LIMIT}'")
        conn.execute(f"SET threads={cls._DUCKDB_THREADS}")

    @staticmethod
    def _get_query_connection_settings(conn) -> tuple[str, int]:
        row = conn.execute(
            """
            SELECT current_setting('memory_limit'),
                   current_setting('threads')
            """
        ).fetchone()
        if row is None:
            raise RuntimeError("无法读取 DuckDB 查询资源设置")
        return str(row[0]), int(row[1])

    @staticmethod
    def _restore_query_connection(conn, settings: tuple[str, int]) -> None:
        memory_limit, threads = settings
        escaped_memory_limit = memory_limit.replace("'", "''")
        conn.execute(f"SET memory_limit='{escaped_memory_limit}'")
        conn.execute(f"SET threads={threads}")

    def load_factor_data(
        self,
        config: BacktestConfig,
        factor_names: tuple[str, ...],
        factor_parameters: Mapping[str, Mapping[str, object]] | None = None,
        minimum_history_days: int = 0,
        data_end_date: date | None = None,
        include_market_cap: bool = False,
        additional_kline_fields: tuple[str, ...] = (),
        financial_signal_dates_only: bool = False,
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
        unknown_kline_fields = set(additional_kline_fields) - self._KLINE_SIGNAL_COLUMNS
        if unknown_kline_fields:
            raise ValueError(
                "不支持的附加行情字段: " + ", ".join(sorted(unknown_kline_fields))
            )
        kline_fields: set[str] = set(additional_kline_fields)
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
                *load_arguments,
                valuation_fields=tuple(sorted(valuation_fields)),
                financial_signal_dates_only=financial_signal_dates_only,
            )
        return self.load_market_data(
            *load_arguments,
            data_end_date=data_end_date,
            valuation_fields=tuple(sorted(valuation_fields)),
            financial_signal_dates_only=financial_signal_dates_only,
        )

    def load_point_in_time_industry(self, points: pd.DataFrame) -> pd.DataFrame:
        """在连接级锁内加载行业点时快照。"""

        with self._duckdb_guard():
            return self._load_point_in_time_industry(points)

    def _load_point_in_time_industry(self, points: pd.DataFrame) -> pd.DataFrame:
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
            frame = (
                conn.execute(
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
                )
                .df()
                .copy(deep=True)
            )
        finally:
            conn.unregister(relation_name)
        frame["date"] = pd.to_datetime(frame["date"])
        frame["symbol"] = frame["symbol"].astype("string")
        return frame

    def get_industry_snapshot_metadata(self) -> dict[str, object]:
        """在连接级锁内读取行业快照摘要。"""

        with self._duckdb_guard():
            return self._get_industry_snapshot_metadata()

    def _get_industry_snapshot_metadata(self) -> dict[str, object]:
        """返回当前历史行业 canonical 快照的可复现摘要。"""

        self.db_manager.ensure_views("industry_classification_sw")
        conn = self.db_manager.get_duckdb_conn()
        snapshot = (
            conn.execute(
                """
            SELECT symbol, effective_date, industry_code, source_updated_date
            FROM industry_classification_sw
            ORDER BY symbol, effective_date, industry_code, source_updated_date
            """
            )
            .df()
            .copy(deep=True)
        )
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
        """在连接级锁内加载基准行情。"""

        with self._duckdb_guard():
            return self._load_benchmark_prices(config)

    def _load_benchmark_prices(self, config: BacktestConfig) -> pd.DataFrame:
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
            .copy(deep=True)
        )
        frame["date"] = pd.to_datetime(frame["date"])
        return frame

    @staticmethod
    def _get_lookback_start(
        conn, start_date, lookback_days: int, calendar_view: str = "daily_kline"
    ):
        if lookback_days <= 0:
            return start_date
        row = conn.execute(
            f"""
            SELECT MIN(date)
            FROM (
                SELECT CAST(date AS DATE) AS date
                FROM {calendar_view}
                WHERE date < ?
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
    def _build_indicator_history_cte(
        indicator_fields: tuple[IndicatorField, ...],
        symbol_relation: str | None = None,
    ) -> str:
        if not indicator_fields:
            return ""
        selected_record_fields = ", ".join(
            f'value_{index} := "{field.source_name}"'
            for index, field in enumerate(indicator_fields)
        )
        selected_record_projection = ",\n                    ".join(
            f"selected_record.value_{index} AS {field.alias}"
            for index, field in enumerate(indicator_fields)
        )
        source_columns = ", ".join(
            f'"{field.source_name}"' for field in indicator_fields
        )
        tie_breaker_parts = [
            "COALESCE(CAST(filename AS VARCHAR), '<NULL>')",
            "COALESCE(CAST(report_date AS VARCHAR), '<NULL>')",
            *[
                f"COALESCE(CAST(\"{field.source_name}\" AS VARCHAR), '<NULL>')"
                for field in indicator_fields
            ],
        ]
        tie_breaker = "md5(concat_ws('|', " + ", ".join(tie_breaker_parts) + "))"
        selection_key = (
            "concat_ws('|', "
            "COALESCE(CAST(report_date AS VARCHAR), '<NULL>'), "
            f"{tie_breaker})"
        )
        symbol_filter = (
            f"\n                WHERE symbol IN (SELECT symbol FROM {symbol_relation})"
            if symbol_relation
            else ""
        )
        return f"""
            WITH indicator_history AS MATERIALIZED (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 8), '%Y%m%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 8), '%Y%m%d')::DATE
                    ) AS pub_date,
                    {source_columns},
                    report_date,
                    {selection_key} AS selection_key
                FROM fin_indicator
                {symbol_filter}
            ),
            canonical_indicator_records AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    arg_max(
                        struct_pack({selected_record_fields}),
                        selection_key
                    ) AS selected_record
                FROM indicator_history
                WHERE pub_date IS NOT NULL
                GROUP BY symbol, pub_date
            ),
            deduplicated_indicators AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    {selected_record_projection}
                FROM canonical_indicator_records
            ),
            indicator_intervals AS MATERIALIZED (
                SELECT
                    *,
                    LEAD(pub_date) OVER (
                        PARTITION BY symbol ORDER BY pub_date
                    ) AS next_pub_date
                FROM deduplicated_indicators
            )
        """

    @staticmethod
    def _build_raw_capital_history_sql(symbol_relation: str) -> str:
        return f"""
            SELECT
                symbol,
                CAST(change_date AS DATE) AS effective_date,
                total_shares
            FROM share_capital
            WHERE CAST(change_date AS DATE) <= ?
              AND symbol IN (SELECT symbol FROM {symbol_relation})
        """

    @staticmethod
    def _build_raw_ttm_history_sql(symbol_relation: str) -> str:
        return f"""
            WITH raw_ttm AS (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST(pub_date AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST(pub_date AS VARCHAR), 8), '%Y%m%d')::DATE
                    ) AS effective_date,
                    report_date,
                    net_profit_ttm,
                    deduct_net_profit_ttm,
                    revenue_ttm,
                    ocf_ttm,
                    filename
                FROM fin_ttm
                WHERE symbol IN (SELECT symbol FROM {symbol_relation})
            )
            SELECT
                symbol,
                effective_date,
                report_date,
                net_profit_ttm,
                deduct_net_profit_ttm,
                revenue_ttm,
                ocf_ttm,
                filename
            FROM raw_ttm
            WHERE effective_date IS NOT NULL
              AND effective_date <= ?
        """

    @staticmethod
    def _build_raw_assets_history_sql(symbol_relation: str) -> str:
        return f"""
            WITH raw_assets AS (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 8), '%Y%m%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 8), '%Y%m%d')::DATE
                    ) AS effective_date,
                    report_date,
                    "归属于母公司股东权益合计" AS net_assets,
                    filename
                FROM fin_balance_sheet
                WHERE symbol IN (SELECT symbol FROM {symbol_relation})
            )
            SELECT symbol, effective_date, report_date, net_assets, filename
            FROM raw_assets
            WHERE effective_date IS NOT NULL
              AND effective_date <= ?
        """

    @staticmethod
    def _build_raw_indicator_history_sql(
        indicator_fields: tuple[IndicatorField, ...], symbol_relation: str
    ) -> str:
        source_columns = ", ".join(
            f'"{field.source_name}"' for field in indicator_fields
        )
        return f"""
            WITH raw_indicator AS (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 8), '%Y%m%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 8), '%Y%m%d')::DATE
                    ) AS pub_date,
                    {source_columns},
                    report_date,
                    filename
                FROM fin_indicator
                WHERE symbol IN (SELECT symbol FROM {symbol_relation})
            )
            SELECT symbol, pub_date, {source_columns}, report_date, filename
            FROM raw_indicator
            WHERE pub_date IS NOT NULL
              AND pub_date <= ?
        """

    @staticmethod
    def _canonicalize_history_rows(
        frame: pd.DataFrame,
        effective_date_column: str,
        value_columns: tuple[str, ...],
    ) -> pd.DataFrame:
        columns = ["symbol", effective_date_column, *value_columns]
        if frame.empty:
            return pd.DataFrame(columns=columns)
        # The raw frame is private to the loader and is discarded immediately
        # after this method returns. Reusing it avoids a second full-size
        # financial-history copy at the peak of the market-data load.
        normalized = frame
        normalized["symbol"] = normalized["symbol"].astype("string")
        normalized[effective_date_column] = pd.to_datetime(
            normalized[effective_date_column], errors="coerce"
        )
        normalized = normalized.loc[normalized[effective_date_column].notna()].copy()
        # Apply the same tie-breakers as the former multi-column sort, one
        # stable pass at a time. Text fields keep lexical ordering; numeric
        # fields keep numeric ordering. Only one temporary key is resident,
        # instead of one full helper column per source field.
        for source_column in reversed(("report_date", "filename", *value_columns)):
            sort_key = normalized[source_column]
            if source_column in {"report_date", "filename"}:
                sort_key = sort_key.astype("string")
            elif not pd.api.types.is_numeric_dtype(sort_key):
                sort_key = pd.to_numeric(sort_key, errors="coerce")
            normalized["_history_sort_key"] = sort_key
            normalized.sort_values(
                "_history_sort_key",
                ascending=False,
                kind="mergesort",
                na_position="last",
                inplace=True,
                ignore_index=True,
            )
            normalized.drop(columns="_history_sort_key", inplace=True)
        normalized.sort_values(
            [effective_date_column, "symbol"],
            ascending=[True, True],
            kind="mergesort",
            inplace=True,
            ignore_index=True,
        )
        normalized.drop_duplicates(
            ["symbol", effective_date_column], keep="first", inplace=True
        )
        return (
            normalized.loc[:, columns]
            .sort_values(["symbol", effective_date_column], kind="mergesort")
            .reset_index(drop=True)
        )

    @staticmethod
    def _fetch_dataframe(conn, query: str, parameters: list[object]) -> pd.DataFrame:
        """读取财务快照并隔离 DuckDB 的结果缓冲区。"""

        result = conn.execute(query, parameters)
        # fetch_df_chunk() requires a follow-up empty fetch to discover the end
        # of the result. On large Parquet scans that extra fetch can invalidate
        # the prior vector buffer. df() materializes the complete result once,
        # while copy() establishes ownership before the next query reuses the
        # DuckDB result buffer.
        detached = result.df().copy(deep=True)
        del result
        return detached

    @classmethod
    def _load_capital_history(
        cls, conn, effective_end_date: date, symbol_relation: str
    ) -> pd.DataFrame:
        frame = cls._fetch_dataframe(
            conn,
            cls._build_raw_capital_history_sql(symbol_relation),
            [effective_end_date],
        )
        if frame.empty:
            return pd.DataFrame(columns=["symbol", "effective_date", "total_shares"])
        frame["symbol"] = frame["symbol"].astype("string")
        frame["effective_date"] = pd.to_datetime(
            frame["effective_date"], errors="coerce"
        )
        frame["total_shares"] = pd.to_numeric(frame["total_shares"], errors="coerce")
        return (
            frame.loc[frame["effective_date"].notna()]
            .groupby(["symbol", "effective_date"], as_index=False, sort=True)[
                "total_shares"
            ]
            .max()
        )

    @classmethod
    def _load_ttm_history(
        cls, conn, effective_end_date: date, symbol_relation: str
    ) -> pd.DataFrame:
        frame = cls._fetch_dataframe(
            conn,
            cls._build_raw_ttm_history_sql(symbol_relation),
            [effective_end_date],
        )
        return cls._canonicalize_history_rows(
            frame,
            "effective_date",
            (
                "net_profit_ttm",
                "deduct_net_profit_ttm",
                "revenue_ttm",
                "ocf_ttm",
            ),
        )

    @classmethod
    def _load_assets_history(
        cls, conn, effective_end_date: date, symbol_relation: str
    ) -> pd.DataFrame:
        frame = cls._fetch_dataframe(
            conn,
            cls._build_raw_assets_history_sql(symbol_relation),
            [effective_end_date],
        )
        return cls._canonicalize_history_rows(frame, "effective_date", ("net_assets",))

    @classmethod
    def _load_indicator_history(
        cls,
        conn,
        indicator_fields: tuple[IndicatorField, ...],
        effective_end_date: date,
        symbol_relation: str,
    ) -> pd.DataFrame:
        frame = cls._fetch_dataframe(
            conn,
            cls._build_raw_indicator_history_sql(indicator_fields, symbol_relation),
            [effective_end_date],
        )
        source_columns = tuple(field.source_name for field in indicator_fields)
        canonical = cls._canonicalize_history_rows(frame, "pub_date", source_columns)
        return canonical.rename(
            columns={field.source_name: field.alias for field in indicator_fields}
        )

    @staticmethod
    def _build_capital_history_sql(symbol_relation: str | None = None) -> str:
        symbol_filter = (
            f"\n                AND symbol IN (SELECT symbol FROM {symbol_relation})"
            if symbol_relation
            else ""
        )
        return f"""
            WITH capital_source AS MATERIALIZED (
                SELECT
                    symbol,
                    CAST(change_date AS DATE) AS effective_date,
                    total_shares
                FROM share_capital
            ),
            canonical_capital AS MATERIALIZED (
                SELECT
                    symbol,
                    effective_date,
                    MAX(total_shares) AS total_shares
                FROM capital_source
                WHERE effective_date IS NOT NULL
                  AND effective_date <= ?
                  {symbol_filter}
                GROUP BY symbol, effective_date
            )
            SELECT symbol, effective_date, total_shares
            FROM canonical_capital
            ORDER BY symbol, effective_date
        """

    @staticmethod
    def _build_ttm_history_sql(symbol_relation: str | None = None) -> str:
        symbol_filter = (
            f"\n                AND symbol IN (SELECT symbol FROM {symbol_relation})"
            if symbol_relation
            else ""
        )
        return f"""
            WITH ttm_source AS MATERIALIZED (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST(pub_date AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST(pub_date AS VARCHAR), 8), '%Y%m%d')::DATE
                    ) AS effective_date,
                    report_date,
                    net_profit_ttm,
                    deduct_net_profit_ttm,
                    revenue_ttm,
                    ocf_ttm,
                    filename
                FROM fin_ttm
            ),
            canonical_ttm AS MATERIALIZED (
                SELECT
                    symbol,
                    effective_date,
                    net_profit_ttm,
                    deduct_net_profit_ttm,
                    revenue_ttm,
                    ocf_ttm
                FROM ttm_source
                WHERE effective_date IS NOT NULL
                  AND effective_date <= ?
                  {symbol_filter}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol, effective_date
                    ORDER BY
                        report_date DESC NULLS LAST,
                        filename DESC NULLS LAST,
                        net_profit_ttm DESC NULLS LAST,
                        deduct_net_profit_ttm DESC NULLS LAST,
                        revenue_ttm DESC NULLS LAST,
                        ocf_ttm DESC NULLS LAST
                ) = 1
            )
            SELECT
                symbol,
                effective_date,
                net_profit_ttm,
                deduct_net_profit_ttm,
                revenue_ttm,
                ocf_ttm
            FROM canonical_ttm
            ORDER BY symbol, effective_date
        """

    @staticmethod
    def _build_assets_history_sql(symbol_relation: str | None = None) -> str:
        symbol_filter = (
            f"\n                AND symbol IN (SELECT symbol FROM {symbol_relation})"
            if symbol_relation
            else ""
        )
        return f"""
            WITH assets_source AS MATERIALIZED (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("数据可用日期" AS VARCHAR), 8), '%Y%m%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 10), '%Y-%m-%d')::DATE,
                        try_strptime(LEFT(CAST("公告日期" AS VARCHAR), 8), '%Y%m%d')::DATE
                    ) AS effective_date,
                    report_date,
                    "归属于母公司股东权益合计" AS net_assets,
                    filename
                FROM fin_balance_sheet
            ),
            canonical_assets AS MATERIALIZED (
                SELECT symbol, effective_date, net_assets
                FROM assets_source
                WHERE effective_date IS NOT NULL
                  AND effective_date <= ?
                  {symbol_filter}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol, effective_date
                    ORDER BY
                        report_date DESC NULLS LAST,
                        filename DESC NULLS LAST,
                        net_assets DESC NULLS LAST
                ) = 1
            )
            SELECT symbol, effective_date, net_assets
            FROM canonical_assets
            ORDER BY symbol, effective_date
        """

    def _build_valuation_frame(
        self,
        conn,
        kline_frame: pd.DataFrame,
        effective_end_date: date,
        valuation_fields: tuple[str, ...],
        symbol_relation: str,
        valuation_histories: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        | None = None,
        drop_missing_rows: bool = True,
    ) -> pd.DataFrame:
        """从独立历史快照按点时回溯构造估值字段。"""

        frame = kline_frame
        if valuation_histories is None:
            valuation_histories = (
                self._load_capital_history(conn, effective_end_date, symbol_relation),
                self._load_ttm_history(conn, effective_end_date, symbol_relation),
                self._load_assets_history(conn, effective_end_date, symbol_relation),
            )
        capital_history, ttm_history, assets_history = valuation_histories
        frame = self._merge_history_point_in_time(
            frame,
            capital_history,
            "effective_date",
            ("total_shares",),
            "capital",
            copy_market_data=False,
            history_is_canonical=True,
            market_data_is_sorted=False,
        )
        if drop_missing_rows:
            frame = frame.loc[frame["_capital_effective_date"].notna()].copy()
            frame.reset_index(drop=True, inplace=True)
        frame.drop(columns=["_capital_effective_date"], inplace=True)
        frame = self._merge_history_point_in_time(
            frame,
            ttm_history,
            "effective_date",
            (
                "net_profit_ttm",
                "deduct_net_profit_ttm",
                "revenue_ttm",
                "ocf_ttm",
            ),
            "ttm",
            copy_market_data=False,
            history_is_canonical=True,
            market_data_is_sorted=True,
        )
        if drop_missing_rows:
            frame = frame.loc[frame["_ttm_effective_date"].notna()].copy()
            frame.reset_index(drop=True, inplace=True)
        frame.drop(columns=["_ttm_effective_date"], inplace=True)
        frame = self._merge_history_point_in_time(
            frame,
            assets_history,
            "effective_date",
            ("net_assets",),
            "assets",
            copy_market_data=False,
            history_is_canonical=True,
            market_data_is_sorted=True,
        )
        if drop_missing_rows:
            frame = frame.loc[frame["_assets_effective_date"].notna()].copy()
            frame.reset_index(drop=True, inplace=True)
        frame.drop(columns=["_assets_effective_date"], inplace=True)

        frame["raw_close"] = pd.to_numeric(frame["raw_close"], errors="coerce")
        frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="coerce")
        frame["close_hfq"] = frame["raw_close"] * frame["adj_factor"]
        frame["market_cap"] = frame["raw_close"] * frame["total_shares"]
        for output, denominator in (
            ("pe_ttm", "net_profit_ttm"),
            ("pe_deduct_ttm", "deduct_net_profit_ttm"),
            ("pb", "net_assets"),
            ("ps_ttm", "revenue_ttm"),
            ("pcf_ttm", "ocf_ttm"),
        ):
            denominator_values = pd.to_numeric(
                frame[denominator], errors="coerce"
            ).where(lambda values: values.ne(0))
            frame[output] = frame["market_cap"] / denominator_values

        output_columns = [
            "date",
            "symbol",
            "raw_close",
            "close_hfq",
            "pe_ttm",
            "pb",
            *valuation_fields,
            "open",
            *[
                column
                for column in kline_frame.columns
                if column
                not in {
                    "date",
                    "symbol",
                    "raw_close",
                    "adj_factor",
                    "open",
                }
            ],
        ]
        output_columns = list(dict.fromkeys(output_columns))
        return frame.loc[:, output_columns]

    @staticmethod
    def _build_kline_projection(kline_fields: tuple[str, ...]) -> str:
        if not kline_fields:
            return ""
        return ", " + ", ".join(
            f'kline."{field}" AS "{field}"' for field in kline_fields
        )

    @staticmethod
    def _canonicalize_kline_rows(frame: pd.DataFrame) -> pd.DataFrame:
        """只在原始行情确有重复 date/symbol 时按视图口径确定性去重。"""

        if frame.empty:
            return frame
        sort_columns = [
            "symbol",
            "date",
            "_kline_filename",
            "_kline_open",
            "_kline_high",
            "_kline_low",
            "_kline_close",
            "_kline_volume",
            "_kline_amount",
            "_kline_adj_factor",
        ]
        missing = set(sort_columns) - set(frame.columns)
        if missing:
            raise ValueError("行情去重字段缺失: " + ", ".join(sorted(missing)))
        normalized = frame.copy()
        normalized = normalized.sort_values(
            sort_columns,
            ascending=[True, True, *([True] * (len(sort_columns) - 2))],
            na_position="last",
            kind="mergesort",
        )
        normalized = normalized.drop_duplicates(["symbol", "date"], keep="first")
        return normalized.drop(
            columns=[column for column in sort_columns if column.startswith("_kline_")]
        ).reset_index(drop=True)

    @staticmethod
    def _merge_market_frames(
        valuation_frame: pd.DataFrame, kline_frame: pd.DataFrame
    ) -> pd.DataFrame:
        """Join already materialized view outputs without a financial range join."""

        key_columns = ["date", "symbol"]
        for name, frame in (("估值", valuation_frame), ("行情", kline_frame)):
            missing = set(key_columns) - set(frame.columns)
            if missing:
                raise ValueError(f"{name}数据缺少字段: " + ", ".join(sorted(missing)))
            if frame.duplicated(key_columns).any():
                raise ValueError(f"{name}数据不能包含重复的 date/symbol")
        merged = valuation_frame.merge(
            kline_frame,
            on=key_columns,
            how="inner",
            validate="one_to_one",
            sort=False,
        )
        return merged

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
    def _merge_indicator_point_in_time(
        market_data: pd.DataFrame,
        indicator_history: pd.DataFrame,
        indicator_fields: tuple[IndicatorField, ...],
        *,
        copy_market_data: bool = True,
        history_is_canonical: bool = False,
        market_data_is_sorted: bool = False,
    ) -> pd.DataFrame:
        """Merge canonical indicator history without a large DuckDB range join."""

        return BacktestDataAccess._merge_history_point_in_time(
            market_data,
            indicator_history,
            "pub_date",
            tuple(field.alias for field in indicator_fields),
            "指标",
            copy_market_data=copy_market_data,
            history_is_canonical=history_is_canonical,
            market_data_is_sorted=market_data_is_sorted,
        )

    @staticmethod
    def _merge_history_point_in_time(
        market_data: pd.DataFrame,
        history: pd.DataFrame,
        effective_date_column: str,
        value_columns: tuple[str, ...],
        history_name: str,
        *,
        copy_market_data: bool = True,
        history_is_canonical: bool = False,
        market_data_is_sorted: bool = False,
    ) -> pd.DataFrame:
        """按每个股票的最新生效日回溯历史值，不使用大型范围连接。"""

        marker_column = f"_{history_name}_effective_date"
        left = market_data.copy() if copy_market_data else market_data
        left["date"] = pd.to_datetime(left["date"], errors="coerce")
        left["symbol"] = left["symbol"].astype("string")
        right = history if history_is_canonical else history.copy()
        if not history_is_canonical:
            right[effective_date_column] = pd.to_datetime(
                right[effective_date_column], errors="coerce"
            )
            right["symbol"] = right["symbol"].astype("string")
            right = right.loc[right[effective_date_column].notna()].copy()
        if right.duplicated(["symbol", effective_date_column]).any():
            raise ValueError(
                f"{history_name}历史不能包含重复的 symbol/{effective_date_column}"
            )
        if not market_data_is_sorted:
            left = left.sort_values(["symbol", "date"], kind="mergesort").reset_index(
                drop=True
            )
        else:
            left.reset_index(drop=True, inplace=True)
        if not history_is_canonical:
            right = right.sort_values(
                ["symbol", effective_date_column], kind="mergesort"
            ).reset_index(drop=True)
        right_groups = {
            symbol: group.reset_index(drop=True)
            for symbol, group in right.groupby("symbol", sort=False)
        }
        value_buffers = {
            column: np.full(len(left), np.nan, dtype="float64")
            for column in value_columns
        }
        marker_buffer = np.full(len(left), np.datetime64("NaT"), dtype="datetime64[ns]")
        if right.empty:
            for column, values in value_buffers.items():
                left[column] = values
            left[marker_column] = pd.to_datetime(marker_buffer)
            return left
        left_dates_all = left["date"].to_numpy(dtype="datetime64[ns]")
        for symbol, left_positions in left.groupby("symbol", sort=False).groups.items():
            right_group = right_groups.get(symbol)
            if right_group is None:
                continue
            left_positions = left_positions.to_numpy(dtype="intp")
            left_dates = left_dates_all[left_positions]
            right_dates = right_group[effective_date_column].to_numpy(
                dtype="datetime64[ns]"
            )
            if (len(right_dates) > 1) and np.any(right_dates[1:] < right_dates[:-1]):
                raise ValueError(f"{history_name}历史生效日期未按升序排列")
            match_positions = np.searchsorted(right_dates, left_dates, side="right") - 1
            valid = (match_positions >= 0) & ~np.isnat(left_dates)
            if not valid.any():
                continue
            matched_left_positions = left_positions[valid]
            matched_right_positions = match_positions[valid]
            matched_dates = right_dates[matched_right_positions]
            if np.any(matched_dates > left_dates[valid]):
                raise ValueError(f"{history_name}历史回溯匹配到了未来生效日期")
            marker_buffer[matched_left_positions] = matched_dates
            for column in value_columns:
                values = pd.to_numeric(right_group[column], errors="coerce").to_numpy(
                    dtype="float64"
                )
                value_buffers[column][matched_left_positions] = values[
                    matched_right_positions
                ]
        for column, values in value_buffers.items():
            left[column] = values
        left[marker_column] = pd.to_datetime(marker_buffer)
        return left
