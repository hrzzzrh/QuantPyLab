from datetime import date

import pandas as pd

import main as main_module
from backtest.data_access import BacktestDataAccess


def _cli_factor_input():
    dates = pd.bdate_range("2024-01-02", periods=4)
    rows = []
    for index in range(10):
        symbol = f"0000{index + 1:02d}"
        for date_index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "open_hfq": 100.0,
                    "close_hfq": 100.0 + index + date_index,
                    "pb": index + 1.0,
                }
            )
    return pd.DataFrame(rows)


def test_run_factor_diagnostics_loads_forward_rows_and_writes_report(
    monkeypatch, tmp_path
):
    captured = {}

    def fake_load_factor_data(
        self,
        config,
        factor_names,
        factor_parameters=None,
        minimum_history_days=0,
        data_end_date=None,
    ):
        captured["factor_names"] = factor_names
        captured["data_end_date"] = data_end_date
        return _cli_factor_input()

    monkeypatch.setattr(BacktestDataAccess, "load_factor_data", fake_load_factor_data)

    output_dir = main_module.run_factor_diagnostics(
        factor_names=["valuation_pb"],
        start_date="2024-01-02",
        end_date="2024-01-03",
        horizons=[1],
        quantile_count=5,
        output_path=str(tmp_path / "diagnostics"),
    )

    assert captured["factor_names"] == ("valuation_pb",)
    assert captured["data_end_date"] == date(2024, 2, 5)
    assert output_dir.joinpath("summary.md").exists()
    assert output_dir.joinpath("parameters.json").exists()
