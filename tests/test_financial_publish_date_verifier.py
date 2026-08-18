"""测试财报公告日期超期二次核验与四源回填。"""

from datetime import date
from pathlib import Path

import pandas as pd

from storage.database import financial_publish_date_reconciler as reconciler_mod
from storage.database.financial_publish_date_verifier import (
    OverduePublishDateVerification,
    apply_official_publish_dates,
    build_current_publish_date_map,
    find_overdue_publish_dates,
    verify_overdue_financial_publish_dates_for_symbol,
)
from storage.database.official_disclosure_date_resolver import OfficialDisclosure

SYMBOL = "000418"
SOURCE_CATEGORIES = reconciler_mod.FINANCIAL_SOURCE_CATEGORIES


def _write_source(
    warehouse: Path,
    source_name: str,
    report_dates: list[str],
    publish_dates: list[str],
) -> None:
    frame = pd.DataFrame(
        {
            "report_date": report_dates,
            "公告日期": publish_dates,
            "业务数值": [10.0 + index for index in range(len(report_dates))],
        }
    )
    path = (
        warehouse / SOURCE_CATEGORIES[source_name] / f"symbol={SYMBOL}" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _write_all_sources(warehouse: Path, publish_date: str = "20241031") -> None:
    for source_name in SOURCE_CATEGORIES:
        _write_source(warehouse, source_name, ["20240930"], [publish_date])


def test_overdue_detection_uses_minimum_current_four_source_date(tmp_path):
    source_frames = {
        "balance": pd.DataFrame(
            {"report_date": ["20240930"], "公告日期": ["20241031"]}
        ),
        "income": pd.DataFrame({"report_date": ["20240930"], "公告日期": ["20241101"]}),
    }

    assert build_current_publish_date_map(source_frames) == {"20240930": "20241031"}
    assert find_overdue_publish_dates(source_frames, date(2026, 8, 17)) == {}

    source_frames["balance"]["公告日期"] = ["20241102"]
    assert find_overdue_publish_dates(source_frames, date(2026, 8, 17)) == {
        "20240930": "20241101"
    }


def test_apply_official_date_updates_only_announcement_date(tmp_path):
    _write_all_sources(tmp_path, "20241102")
    frames = {
        source_name: pd.read_parquet(
            tmp_path / category / f"symbol={SYMBOL}" / "data.parquet"
        )
        for source_name, category in SOURCE_CATEGORIES.items()
    }
    official_dates = {
        "20240930": OfficialDisclosure(
            date(2024, 10, 30), "公司2024年第三季度报告", "", "cninfo"
        )
    }

    changed = apply_official_publish_dates(
        SYMBOL, official_dates, frames, warehouse_dir=tmp_path
    )

    assert changed == {source_name: 1 for source_name in SOURCE_CATEGORIES}
    for source_name, category in SOURCE_CATEGORIES.items():
        result = pd.read_parquet(
            tmp_path / category / f"symbol={SYMBOL}" / "data.parquet"
        )
        assert result["公告日期"].tolist() == ["20241030"]
        assert result["业务数值"].tolist() == [10.0]


def test_verify_overdue_dates_queries_resolver_and_returns_changes(tmp_path):
    _write_all_sources(tmp_path, "20241102")

    class FakeResolver:
        def __init__(self):
            self.calls = []

        def resolve_overdue_report_dates(self, symbol, report_dates, end_date):
            self.calls.append((symbol, report_dates, end_date))
            return {
                "20240930": OfficialDisclosure(
                    date(2024, 10, 30), "公司2024年第三季度报告", "", "cninfo"
                )
            }

    resolver = FakeResolver()
    result = verify_overdue_financial_publish_dates_for_symbol(
        SYMBOL,
        resolver=resolver,
        warehouse_dir=tmp_path,
        today=date(2026, 8, 17),
    )

    assert isinstance(result, OverduePublishDateVerification)
    assert result.overdue_report_dates == ("20240930",)
    assert result.unresolved_report_dates == ()
    assert result.changed_rows == {source_name: 1 for source_name in SOURCE_CATEGORIES}
    assert resolver.calls[0][1] == ("20240930",)


def test_verify_api_failure_keeps_existing_date(tmp_path):
    _write_all_sources(tmp_path, "20241102")

    class FailingResolver:
        def resolve_overdue_report_dates(self, *args, **kwargs):
            from storage.database.official_disclosure_date_resolver import (
                OfficialDisclosureQueryError,
            )

            raise OfficialDisclosureQueryError("接口不可用")

    result = verify_overdue_financial_publish_dates_for_symbol(
        SYMBOL,
        resolver=FailingResolver(),
        warehouse_dir=tmp_path,
        today=date(2026, 8, 17),
    )

    assert result.unresolved_report_dates == ("20240930",)
    assert result.changed_rows == {}
    result_frame = pd.read_parquet(
        tmp_path / SOURCE_CATEGORIES["income"] / f"symbol={SYMBOL}" / "data.parquet"
    )
    assert result_frame["公告日期"].tolist() == ["20241102"]


def test_verify_keeps_official_date_and_warns_when_still_overdue(tmp_path, caplog):
    _write_all_sources(tmp_path, "20241102")

    class LateResolver:
        def resolve_overdue_report_dates(self, *args, **kwargs):
            return {
                "20240930": OfficialDisclosure(
                    date(2024, 11, 5), "公司2024年第三季度报告", "", "cninfo"
                )
            }

    result = verify_overdue_financial_publish_dates_for_symbol(
        SYMBOL,
        resolver=LateResolver(),
        warehouse_dir=tmp_path,
        today=date(2026, 8, 17),
    )

    assert result.unresolved_report_dates == ()
    assert result.changed_rows == {source_name: 1 for source_name in SOURCE_CATEGORIES}
    assert "仍晚于法定截止日" in caplog.text
    result_frame = pd.read_parquet(
        tmp_path / SOURCE_CATEGORIES["income"] / f"symbol={SYMBOL}" / "data.parquet"
    )
    assert result_frame["公告日期"].tolist() == ["20241105"]
