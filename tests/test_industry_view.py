import duckdb
import pandas as pd

from storage.database.views.market.industry_classification_sw import (
    IndustryClassificationShenwanView,
)


def test_industry_view_reads_symbol_from_partition_and_exposes_effective_date(
    tmp_path,
):
    partition = tmp_path / "industry_classification_sw" / "symbol=000001"
    partition.mkdir(parents=True)
    pd.DataFrame(
        {
            "effective_date": [
                pd.Timestamp("2020-01-01").date(),
                pd.Timestamp("2022-01-01").date(),
            ],
            "industry_code": ["480101", "480301"],
            "source_updated_date": [
                pd.Timestamp("2025-12-15").date(),
                pd.Timestamp("2025-12-15").date(),
            ],
        }
    ).to_parquet(partition / "data.parquet", index=False)

    conn = duckdb.connect(":memory:")
    conn.execute(IndustryClassificationShenwanView().get_sql(str(tmp_path)))
    result = conn.execute(
        """
        SELECT symbol, effective_date, industry_code
        FROM industry_classification_sw
        ORDER BY effective_date
        """
    ).fetchdf()

    assert result.to_dict("records") == [
        {
            "symbol": "000001",
            "effective_date": pd.Timestamp("2020-01-01"),
            "industry_code": "480101",
        },
        {
            "symbol": "000001",
            "effective_date": pd.Timestamp("2022-01-01"),
            "industry_code": "480301",
        },
    ]

    signals = pd.DataFrame(
        {
            "symbol": ["000001"] * 3,
            "signal_date": pd.to_datetime(["2019-12-31", "2020-01-01", "2022-01-01"]),
        }
    )
    conn.register("signals", signals)
    asof_result = conn.execute(
        """
        SELECT signals.signal_date, industry.industry_code
        FROM (SELECT * FROM signals ORDER BY symbol, signal_date) AS signals
        ASOF LEFT JOIN (
            SELECT *
            FROM industry_classification_sw
            ORDER BY symbol, effective_date
        ) AS industry
          ON signals.symbol = industry.symbol
         AND signals.signal_date >= industry.effective_date
        ORDER BY signals.signal_date
        """
    ).fetchdf()

    assert pd.isna(asof_result.loc[0, "industry_code"])
    assert asof_result["industry_code"].iloc[1:].tolist() == ["480101", "480301"]
