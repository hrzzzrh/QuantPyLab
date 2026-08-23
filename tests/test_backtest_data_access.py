from datetime import date

import duckdb
import pandas as pd

from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess, IndicatorField


def _config():
    return BacktestConfig(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 1),
        strategy_name="multi-factor-quality-value-momentum",
        benchmark_symbol=None,
    )


def test_load_factor_data_collects_registered_input_requirements(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
        financial_signal_dates_only=False,
    ):
        captured["config"] = config
        captured["lookback_days"] = lookback_days
        captured["indicator_fields"] = indicator_fields
        captured["kline_fields"] = kline_fields
        captured["valuation_fields"] = valuation_fields
        captured["financial_signal_dates_only"] = financial_signal_dates_only
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    result = access.load_factor_data(
        _config(),
        (
            "price_momentum_120d",
            "quality_roe_weighted",
            "quality_operating_cashflow_ratio",
        ),
        factor_parameters={"price_momentum_120d": {"lookback_days": 300}},
        minimum_history_days=250,
    )

    assert result.empty
    assert captured["lookback_days"] == 300
    assert captured["kline_fields"] == ()
    assert captured["valuation_fields"] == ()
    assert captured["financial_signal_dates_only"] is False
    assert captured["indicator_fields"] == (
        IndicatorField("净资产收益率_加权", "roe_weighted"),
        IndicatorField("经营现金流/营业收入", "operating_cashflow_to_revenue"),
    )


def test_load_factor_data_forwards_sparse_financial_signal_mode(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
        financial_signal_dates_only=False,
    ):
        del config, lookback_days, indicator_fields, kline_fields
        del data_end_date, valuation_fields
        captured["financial_signal_dates_only"] = financial_signal_dates_only
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    access.load_factor_data(
        _config(),
        ("price_momentum_120d",),
        financial_signal_dates_only=True,
    )

    assert captured["financial_signal_dates_only"] is True


def test_configure_query_connection_sets_bounded_duckdb_resources():
    class FakeConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)

    connection = FakeConnection()

    BacktestDataAccess._configure_query_connection(connection)

    assert connection.statements == [
        "SET memory_limit='256MB'",
        "SET threads=2",
    ]


def test_fetch_dataframe_detaches_duckdb_chunk_buffer():
    shared_frame = pd.DataFrame({"value": [1.0, 2.0]})

    class FakeResult:
        def df(self):
            return shared_frame

    class FakeConnection:
        def execute(self, query, parameters):
            del query, parameters
            return FakeResult()

    result = BacktestDataAccess._fetch_dataframe(FakeConnection(), "SELECT value", [])
    shared_frame.loc[:, "value"] = [99.0, 100.0]

    assert result["value"].tolist() == [1.0, 2.0]


def test_load_market_data_restores_resources_when_calendar_query_fails(monkeypatch):
    class FakeConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)
            return self

        def fetchone(self):
            return ("1GB", 8)

    class FakeManager:
        def __init__(self, connection):
            self.connection = connection

        def ensure_views(self, *view_names):
            del view_names

        def get_duckdb_conn(self):
            return self.connection

    connection = FakeConnection()
    access = BacktestDataAccess(FakeManager(connection))

    def fail_calendar_query(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("calendar query failed")

    monkeypatch.setattr(
        BacktestDataAccess,
        "_get_lookback_start",
        staticmethod(fail_calendar_query),
    )

    try:
        access.load_market_data(_config(), lookback_days=20)
    except RuntimeError as error:
        assert str(error) == "calendar query failed"
    else:
        raise AssertionError("日历查询失败应该向调用方抛出")

    assert connection.statements[-2:] == [
        "SET memory_limit='1GB'",
        "SET threads=8",
    ]


def test_sparse_financial_market_load_preserves_batches_and_point_in_time_values(
    monkeypatch,
):
    dates = pd.to_datetime(["2024-01-30", "2024-01-31", "2024-02-28", "2024-02-29"])
    kline_rows = []
    for symbol_index, symbol in enumerate(("000001", "000002", "000003"), start=1):
        for day_index, current_date in enumerate(dates, start=1):
            kline_rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close": float(10 * symbol_index + day_index),
                    "adj_factor": 1.0,
                    "open": float(10 * symbol_index + day_index - 0.5),
                    "high": float(10 * symbol_index + day_index + 1),
                    "low": float(10 * symbol_index + day_index - 1),
                    "volume": 100.0,
                    "amount": 1000.0,
                    "filename": f"{symbol}-canonical.parquet",
                }
            )
    duplicate = dict(kline_rows[7])
    duplicate["close"] = 999.0
    duplicate["filename"] = "zz-duplicate.parquet"
    kline_rows.append(duplicate)
    kline = pd.DataFrame(kline_rows)

    capital = pd.DataFrame(
        {
            "symbol": ["000001", "000002", "000003"],
            "change_date": pd.to_datetime(["2024-01-01"] * 3),
            "total_shares": [100.0, 100.0, 100.0],
        }
    )
    ttm = pd.DataFrame(
        {
            "symbol": ["000001", "000002", "000003"],
            "pub_date": ["2024-01-15", "2024-02-15", "2024-03-01"],
            "report_date": ["2023-12-31"] * 3,
            "net_profit_ttm": [100.0, 200.0, 300.0],
            "deduct_net_profit_ttm": [100.0, 200.0, 300.0],
            "revenue_ttm": [1000.0, 2000.0, 3000.0],
            "ocf_ttm": [80.0, 160.0, 240.0],
            "filename": ["ttm-a", "ttm-b", "ttm-c"],
        }
    )
    assets = pd.DataFrame(
        {
            "symbol": ["000001", "000002", "000003"],
            "数据可用日期": ["2024-01-15", "2024-02-15", "2024-03-01"],
            "公告日期": ["2024-01-15", "2024-02-15", "2024-03-01"],
            "report_date": ["2023-12-31"] * 3,
            "归属于母公司股东权益合计": [500.0, 600.0, 700.0],
            "filename": ["assets-a", "assets-b", "assets-c"],
        }
    )

    connection = duckdb.connect()
    connection.register("daily_kline_raw", kline)
    connection.register("daily_kline_calendar", kline.loc[:, ["date", "symbol"]])
    connection.register("share_capital", capital)
    connection.register("fin_ttm", ttm)
    connection.register("fin_balance_sheet", assets)

    class InMemoryManager:
        def ensure_views(self, *view_names):
            del view_names

        def get_duckdb_conn(self):
            return connection

        def close_duckdb(self):
            return None

    monkeypatch.setattr(BacktestDataAccess, "_KLINE_SYMBOL_BATCH_SIZE", 2)
    access = BacktestDataAccess(InMemoryManager())
    try:
        result = access.load_market_data(
            _config(),
            lookback_days=0,
            data_end_date=date(2024, 2, 29),
            financial_signal_dates_only=True,
        )
    finally:
        connection.close()

    assert len(result) == 12
    assert not result.duplicated(["date", "symbol"]).any()
    assert set(result["symbol"].astype("string")) == {"000001", "000002", "000003"}
    duplicate_key = (result["date"] == pd.Timestamp("2024-02-29")) & (
        result["symbol"].astype("string") == "000002"
    )
    assert result.loc[duplicate_key, "raw_close"].item() == 24.0

    non_signal_rows = result[
        ~result["date"].isin(pd.to_datetime(["2024-01-31", "2024-02-29"]))
    ]
    assert non_signal_rows[["pe_ttm", "pb"]].isna().all().all()
    january_symbol_one = result[
        (result["date"] == pd.Timestamp("2024-01-31"))
        & (result["symbol"].astype("string") == "000001")
    ]
    assert january_symbol_one["pe_ttm"].notna().all()
    january_symbol_two = result[
        (result["date"] == pd.Timestamp("2024-01-31"))
        & (result["symbol"].astype("string") == "000002")
    ]
    assert january_symbol_two[["pe_ttm", "pb"]].isna().all().all()
    february_symbol_two = result[
        (result["date"] == pd.Timestamp("2024-02-29"))
        & (result["symbol"].astype("string") == "000002")
    ]
    assert february_symbol_two[["pe_ttm", "pb"]].notna().all().all()
    symbol_three = result[result["symbol"].astype("string") == "000003"]
    assert symbol_three[["pe_ttm", "pb"]].isna().all().all()


def test_load_factor_data_collects_new_factor_input_requirements(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
        financial_signal_dates_only=False,
    ):
        captured["lookback_days"] = lookback_days
        captured["indicator_fields"] = indicator_fields
        captured["valuation_fields"] = valuation_fields
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    access.load_factor_data(
        _config(),
        (
            "valuation_ps_ttm",
            "valuation_pcf_ttm",
            "growth_revenue_yoy",
            "growth_deduct_profit_yoy",
            "quality_roic",
            "price_reversal_20d",
        ),
        factor_parameters={"price_reversal_20d": {"lookback_days": 20}},
    )

    assert captured["lookback_days"] == 20
    assert captured["valuation_fields"] == ("pcf_ttm", "ps_ttm")
    assert captured["indicator_fields"] == (
        IndicatorField("营业总收入同比增长", "revenue_yoy"),
        IndicatorField("扣非净利润同比增长", "deduct_profit_yoy"),
        IndicatorField("投入资本回报率", "roic"),
    )


def test_load_factor_data_can_include_forward_price_rows(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
        financial_signal_dates_only=False,
    ):
        captured["data_end_date"] = data_end_date
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    access.load_factor_data(
        _config(),
        ("price_momentum_120d",),
        data_end_date=date(2024, 3, 1),
    )

    assert captured["data_end_date"] == date(2024, 3, 1)


def test_load_factor_data_can_include_additional_kline_fields(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
        financial_signal_dates_only=False,
    ):
        captured["kline_fields"] = kline_fields
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    access.load_factor_data(
        _config(),
        ("price_momentum_120d",),
        additional_kline_fields=("amount", "volume"),
    )

    assert captured["kline_fields"] == ("amount", "volume")


def test_load_factor_data_rejects_unknown_additional_kline_field():
    access = BacktestDataAccess(object())

    try:
        access.load_factor_data(
            _config(),
            ("price_momentum_120d",),
            additional_kline_fields=("turnover_value",),
        )
    except ValueError as error:
        assert "不支持的附加行情字段" in str(error)
    else:
        raise AssertionError("未知附加行情字段应该被拒绝")


def test_load_factor_data_rejects_empty_factor_list():
    access = BacktestDataAccess(object())

    try:
        access.load_factor_data(_config(), ())
    except ValueError as error:
        assert "至少需要指定一个因子" in str(error)
    else:
        raise AssertionError("空因子列表应该被拒绝")


def test_build_kline_projection_quotes_requested_columns():
    projection = BacktestDataAccess._build_kline_projection(("volume", "amount"))

    assert projection == ', kline."volume" AS "volume", kline."amount" AS "amount"'


def test_canonicalize_kline_rows_matches_daily_view_tie_break_order():
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-03"]),
            "symbol": ["000001", "000001", "000001"],
            "raw_close": [101.0, 102.0, 103.0],
            "adj_factor": [1.0, 1.0, 1.0],
            "open": [101.0, 102.0, 103.0],
            "_kline_open": [101.0, 102.0, 103.0],
            "_kline_high": [101.0, 102.0, 103.0],
            "_kline_low": [101.0, 102.0, 103.0],
            "_kline_close": [101.0, 102.0, 103.0],
            "_kline_volume": [100.0, 100.0, 100.0],
            "_kline_amount": [100.0, 100.0, 100.0],
            "_kline_adj_factor": [1.0, 1.0, 1.0],
            "_kline_filename": ["b.parquet", "a.parquet", "c.parquet"],
        }
    )

    result = BacktestDataAccess._canonicalize_kline_rows(frame)

    assert result[["date", "raw_close"]].to_dict("records") == [
        {"date": pd.Timestamp("2024-01-02"), "raw_close": 102.0},
        {"date": pd.Timestamp("2024-01-03"), "raw_close": 103.0},
    ]
    assert not any(column.startswith("_kline_") for column in result.columns)


def test_canonicalize_history_rows_sorts_numeric_values_numerically():
    frame = pd.DataFrame(
        {
            "symbol": ["000001", "000001"],
            "effective_date": ["2024-01-02", "2024-01-02"],
            "report_date": ["2024-01-01", "2024-01-01"],
            "filename": ["same.parquet", "same.parquet"],
            "value": [9.0, 10.0],
        }
    )

    result = BacktestDataAccess._canonicalize_history_rows(
        frame, "effective_date", ("value",)
    )

    assert result["value"].tolist() == [10.0]


def test_merge_indicator_point_in_time_uses_latest_available_row():
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2022-04-29", "2022-08-25"]),
            "symbol": ["000001", "000001"],
            "raw_close": [10.0, 11.0],
        }
    )
    indicator_history = pd.DataFrame(
        {
            "symbol": ["000001", "000001"],
            "pub_date": pd.to_datetime(["2022-04-28", "2022-08-25"]),
            "roe": [1.0, 2.0],
        }
    )

    merged = BacktestDataAccess._merge_indicator_point_in_time(
        market_data,
        indicator_history,
        (IndicatorField("净资产收益率", "roe"),),
    )

    assert merged["roe"].tolist() == [1.0, 2.0]


def test_merge_indicator_point_in_time_rejects_duplicate_history_keys():
    market_data = pd.DataFrame(
        {"date": pd.to_datetime(["2022-04-29"]), "symbol": ["000001"]}
    )
    indicator_history = pd.DataFrame(
        {
            "symbol": ["000001", "000001"],
            "pub_date": pd.to_datetime(["2022-04-28", "2022-04-28"]),
            "roe": [1.0, 2.0],
        }
    )

    try:
        BacktestDataAccess._merge_indicator_point_in_time(
            market_data,
            indicator_history,
            (IndicatorField("净资产收益率", "roe"),),
        )
    except ValueError as error:
        assert "重复的 symbol/pub_date" in str(error)
    else:
        raise AssertionError("重复指标历史键应该被拒绝")


def test_merge_history_point_in_time_never_uses_future_rows():
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2022-07-29", "2023-04-28"]),
            "symbol": ["688046", "688046"],
        }
    )
    history = pd.DataFrame(
        {
            "symbol": ["688046", "688046", "688046"],
            "effective_date": pd.to_datetime(
                ["2022-04-22", "2022-08-20", "2023-04-28"]
            ),
            "net_assets": [657.0, 707.0, 816.0],
        }
    )

    merged = BacktestDataAccess._merge_history_point_in_time(
        market_data,
        history,
        "effective_date",
        ("net_assets",),
        "assets",
    )

    assert merged["net_assets"].tolist() == [657.0, 816.0]
    assert merged["_assets_effective_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2022-04-22",
        "2023-04-28",
    ]


def test_merge_history_point_in_time_handles_non_contiguous_sorted_index():
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2022-07-29", "2023-04-28"]),
            "symbol": ["688046", "688046"],
        },
        index=[10, 20],
    )
    history = pd.DataFrame(
        {
            "symbol": ["688046", "688046"],
            "effective_date": pd.to_datetime(["2022-04-22", "2023-04-28"]),
            "net_assets": [657.0, 816.0],
        }
    )

    merged = BacktestDataAccess._merge_history_point_in_time(
        market_data,
        history,
        "effective_date",
        ("net_assets",),
        "assets",
        market_data_is_sorted=True,
    )

    assert merged["net_assets"].tolist() == [657.0, 816.0]


def test_merge_history_point_in_time_rejects_duplicate_effective_dates():
    market_data = pd.DataFrame(
        {"date": pd.to_datetime(["2022-07-29"]), "symbol": ["000001"]}
    )
    history = pd.DataFrame(
        {
            "symbol": ["000001", "000001"],
            "effective_date": pd.to_datetime(["2022-04-28", "2022-04-28"]),
            "value": [1.0, 2.0],
        }
    )

    try:
        BacktestDataAccess._merge_history_point_in_time(
            market_data, history, "effective_date", ("value",), "assets"
        )
    except ValueError as error:
        assert "重复的 symbol/effective_date" in str(error)
    else:
        raise AssertionError("重复历史生效键应该被拒绝")


def test_valuation_history_sql_canonicalizes_same_effective_date():
    ttm_sql = BacktestDataAccess._build_ttm_history_sql()
    assets_sql = BacktestDataAccess._build_assets_history_sql()

    assert "PARTITION BY symbol, effective_date" in ttm_sql
    assert "ROW_NUMBER() OVER" in ttm_sql
    assert "PARTITION BY symbol, effective_date" in assets_sql
    assert "ROW_NUMBER() OVER" in assets_sql


def test_build_valuation_projection_includes_only_requested_extensions():
    projection = BacktestDataAccess._build_valuation_projection(("pcf_ttm", "ps_ttm"))

    assert "valuation.pe_ttm" in projection
    assert "valuation.pb" in projection
    assert "valuation.pcf_ttm AS pcf_ttm" in projection
    assert "valuation.ps_ttm AS ps_ttm" in projection

    try:
        BacktestDataAccess._build_valuation_projection(("unknown",))
    except ValueError as error:
        assert "不支持的估值信号字段" in str(error)
    else:
        raise AssertionError("未知估值字段应该被拒绝")
