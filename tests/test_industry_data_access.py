import duckdb
import pandas as pd
import pytest

from backtest.data_access import BacktestDataAccess
from storage.database.views.market.industry_classification_sw import (
    IndustryClassificationShenwanView,
)


def test_load_point_in_time_industry_uses_asof_view(tmp_path):
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

    class FakeDBManager:
        def ensure_views(self, *view_names):
            assert view_names == ("industry_classification_sw",)

        def get_duckdb_conn(self):
            return conn

    points = pd.DataFrame(
        {
            "date": pd.to_datetime(["2019-12-31", "2020-01-01", "2022-01-01"]),
            "symbol": ["000001"] * 3,
        }
    )
    result = BacktestDataAccess(FakeDBManager()).load_point_in_time_industry(points)

    assert pd.isna(result.loc[0, "industry_code"])
    assert result["industry_code"].iloc[1:].tolist() == ["480101", "480301"]
    metadata = BacktestDataAccess(FakeDBManager()).get_industry_snapshot_metadata()
    assert metadata["row_count"] == 2
    assert metadata["symbol_count"] == 1
    assert len(metadata["snapshot_sha256"]) == 64
    assert metadata["source_updated_date_min"] == "2025-12-15"
    assert metadata["source_updated_date_max"] == "2025-12-15"


def test_load_point_in_time_industry_rejects_duplicate_points():
    points = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "symbol": ["000001", "000001"],
        }
    )

    with pytest.raises(ValueError, match="重复"):
        BacktestDataAccess(object()).load_point_in_time_industry(points)
