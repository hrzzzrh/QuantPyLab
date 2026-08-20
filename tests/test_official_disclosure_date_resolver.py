"""测试巨潮与北交所官方定期报告公告日期解析。"""

from datetime import date

import pytest
import requests

import storage.database.official_disclosure_date_resolver as resolver_mod
from storage.database.official_disclosure_date_resolver import (
    BseDisclosureClient,
    CninfoDisclosureClient,
    OfficialDisclosure,
    OfficialDisclosureDateResolver,
    OfficialDisclosureQueryError,
    get_disclosure_deadline,
    parse_date_value,
    select_first_disclosure,
    title_matches_report,
    title_matches_report_summary,
)


class _FakeResponse:
    def __init__(self, payload=None, text=None):
        self._payload = payload
        self.text = text

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_report_deadlines_and_numeric_date_parsing():
    assert get_disclosure_deadline("20240331") == date(2024, 4, 30)
    assert get_disclosure_deadline("20240630") == date(2024, 8, 31)
    assert get_disclosure_deadline("20240930") == date(2024, 10, 31)
    assert get_disclosure_deadline("20241231") == date(2025, 4, 30)
    assert parse_date_value(20240430) == date(2024, 4, 30)


def test_title_matching_excludes_non_original_documents():
    assert title_matches_report("公司2024年第一季度报告", "20240331")
    assert not title_matches_report("公司2024年第一季度报告摘要", "20240331")
    assert not title_matches_report("公司2024年第一季度报告（英文版）", "20240331")
    assert not title_matches_report("公司2024年第一季度报告更正公告", "20240331")

    announcements = [
        OfficialDisclosure(date(2024, 4, 30), "公司2024年第一季度报告", "late", "test"),
        OfficialDisclosure(
            date(2024, 4, 25), "公司2024年第一季度报告摘要", "summary", "test"
        ),
        OfficialDisclosure(
            date(2024, 4, 26), "公司2024年第一季度报告", "early", "test"
        ),
    ]
    assert select_first_disclosure(announcements, "20240331").url == "early"


def test_summary_is_fallback_only_when_original_report_is_missing():
    summary = OfficialDisclosure(
        date(2012, 4, 28), "公司2011年年度报告摘要", "summary", "test"
    )
    corrected_summary = OfficialDisclosure(
        date(2012, 4, 29), "公司2011年年度报告摘要（修订版）", "corrected", "test"
    )

    assert not title_matches_report(summary.title, "20111231")
    assert title_matches_report_summary(summary.title, "20111231")
    assert select_first_disclosure([corrected_summary], "20111231") is None
    assert select_first_disclosure([summary], "20111231") == summary


def test_cninfo_client_resolves_org_and_parses_announcements():
    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.calls = []

        def post(self, url, data, timeout):
            self.calls.append((url, data, timeout))
            if "topSearch" in url:
                return _FakeResponse([{"code": "600519", "orgId": "gssh0600519"}])
            return _FakeResponse(
                {
                    "announcements": [
                        {
                            "announcementTime": "2024-04-26 00:00:00",
                            "announcementTitle": "贵州茅台2024年第一季度报告",
                            "adjunctUrl": "2024-04-26/abc.PDF",
                        }
                    ],
                    "totalRecord": 1,
                }
            )

    session = FakeSession()
    client = CninfoDisclosureClient(session=session)

    result = client.fetch_announcements("600519", date(2024, 3, 31), date(2024, 5, 1))

    assert result[0].publish_date == date(2024, 4, 26)
    assert result[0].url.endswith("2024-04-26/abc.PDF")
    assert session.calls[1][1]["stock"] == "600519,gssh0600519"
    assert session.calls[1][1]["column"] == "sse"
    assert session.calls[1][1]["category"] == "category_ndbg_szsh"


def test_cninfo_client_uses_total_record_for_pagination():
    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.page_numbers = []

        def post(self, url, data, timeout):
            if "topSearch" in url:
                return _FakeResponse([{"code": "600519", "orgId": "gssh0600519"}])
            page_number = int(data["pageNum"])
            self.page_numbers.append(page_number)
            if page_number == 1:
                items = [
                    {
                        "announcementTime": "2024-04-01",
                        "announcementTitle": "公司2024年其他公告A",
                        "adjunctUrl": "a.pdf",
                    },
                    {
                        "announcementTime": "2024-04-02",
                        "announcementTitle": "公司2024年其他公告B",
                        "adjunctUrl": "b.pdf",
                    },
                ]
            else:
                items = [
                    {
                        "announcementTime": "2024-04-26",
                        "announcementTitle": "公司2024年第一季度报告",
                        "adjunctUrl": "q1.pdf",
                    }
                ]
            return _FakeResponse({"announcements": items, "totalRecord": 3})

    session = FakeSession()
    result = CninfoDisclosureClient(session=session, page_size=2).fetch_announcements(
        "600519", date(2024, 3, 31), date(2024, 5, 1)
    )

    assert session.page_numbers == [1, 2, 1, 2, 1, 2, 1, 2]
    assert result[-1].title == "公司2024年第一季度报告"


def test_cninfo_client_queries_each_report_category_for_historical_records():
    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.categories = []

        def post(self, url, data, timeout):
            if "topSearch" in url:
                return _FakeResponse([{"code": "600519", "orgId": "gssh0600519"}])
            self.categories.append(data["category"])
            title = (
                "公司2010年年度报告"
                if data["category"] == "category_ndbg_szsh"
                else "公司2010年其他公告"
            )
            return _FakeResponse(
                {
                    "announcements": [
                        {
                            "announcementTime": "2011-03-01",
                            "announcementTitle": title,
                            "adjunctUrl": f"{data['category']}.pdf",
                        }
                    ]
                }
            )

    session = FakeSession()
    result = CninfoDisclosureClient(session=session).fetch_announcements(
        "600519", date(2010, 1, 1), date(2026, 8, 19)
    )

    assert session.categories == [
        "category_ndbg_szsh",
        "category_bndbg_szsh",
        "category_yjdbg_szsh",
        "category_sjdbg_szsh",
    ]
    assert select_first_disclosure(result, "20101231").publish_date == date(2011, 3, 1)


def test_cninfo_client_treats_null_announcements_as_empty():
    class FakeSession:
        def __init__(self):
            self.headers = {}

        def post(self, url, data, timeout):
            if "topSearch" in url:
                return _FakeResponse([{"code": "000003", "orgId": "gssz0000003"}])
            return _FakeResponse({"announcements": None})

    result = CninfoDisclosureClient(session=FakeSession()).fetch_announcements(
        "000003", date(2010, 1, 1), date(2026, 8, 19)
    )

    assert result == []


def test_cninfo_client_rejects_pagination_safety_limit():
    class FakeSession:
        def __init__(self):
            self.headers = {}

        def post(self, url, data, timeout):
            if "topSearch" in url:
                return _FakeResponse([{"code": "600519", "orgId": "gssh0600519"}])
            return _FakeResponse(
                {
                    "announcements": [
                        {
                            "announcementTime": "2024-04-01",
                            "announcementTitle": "公司2024年其他公告",
                            "adjunctUrl": "a.pdf",
                        }
                    ],
                    "totalRecord": 2,
                }
            )

    with pytest.raises(OfficialDisclosureQueryError, match="分页超过安全上限"):
        CninfoDisclosureClient(
            session=FakeSession(), page_size=1, max_pages=1
        ).fetch_announcements("600519", date(2024, 3, 31), date(2024, 5, 1))


@pytest.mark.parametrize(
    ("report_date", "title"),
    [
        ("20240331", "公司2024年第一季度报告"),
        ("20240630", "公司2024年半年度报告"),
        ("20240930", "公司2024年第三季度报告"),
        ("20241231", "公司2024年年度报告"),
    ],
)
def test_title_matching_supports_all_period_types(report_date, title):
    assert title_matches_report(title, report_date)


def test_bse_client_normalizes_legacy_code_and_parses_jsonp():
    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.calls = []

        def post(self, url, data, timeout):
            self.calls.append((url, data, timeout))
            return _FakeResponse(
                text=(
                    'null([{"listInfo":{"content":[{"companyCd":"920799",'
                    '"disclosureTitle":"公司2024年第一季度报告",'
                    '"publishDate":"2024-04-26",'
                    '"destFilePath":"/disclosure/abc.PDF"}],'
                    '"totalElements":1}}])'
                )
            )

    session = FakeSession()
    client = BseDisclosureClient(session=session)

    result = client.fetch_announcements("830799", date(2024, 3, 31), date(2024, 5, 1))

    assert result[0].publish_date == date(2024, 4, 26)
    assert result[0].url == "https://www.bse.cn/disclosure/abc.PDF"
    assert session.calls[0][1]["companyCd"] == "920799"


def test_bse_client_uses_total_elements_for_pagination():
    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.pages = []

        def post(self, url, data, timeout):
            self.pages.append(int(data["page"]))
            page = int(data["page"])
            if page == 0:
                content = [
                    {
                        "companyCd": "920799",
                        "disclosureTitle": f"公司2024年其他公告{index}",
                        "publishDate": "2024-04-01",
                        "destFilePath": f"/disclosure/{index}.pdf",
                    }
                    for index in range(2)
                ]
            else:
                content = [
                    {
                        "companyCd": "920799",
                        "disclosureTitle": "公司2024年第一季度报告",
                        "publishDate": "2024-04-26",
                        "destFilePath": "/disclosure/q1.pdf",
                    }
                ]
            return _FakeResponse(
                text=(
                    'null([{"listInfo":{"content":'
                    + str(content).replace("'", '"')
                    + ',"totalElements":3}}])'
                )
            )

    session = FakeSession()
    result = BseDisclosureClient(session=session, page_size=2).fetch_announcements(
        "830799", date(2024, 3, 31), date(2024, 5, 1)
    )

    assert session.pages == [0, 1]
    assert result[-1].title == "公司2024年第一季度报告"
    assert session.pages[0] == 0


def test_bse_client_rejects_pagination_safety_limit():
    class FakeSession:
        def __init__(self):
            self.headers = {}

        def post(self, url, data, timeout):
            return _FakeResponse(
                text=(
                    'null([{"listInfo":{"content":[{"publishDate":"2024-04-01",'
                    '"disclosureTitle":"公司2024年其他公告",'
                    '"destFilePath":"/disclosure/a.pdf"}],"totalElements":2}}])'
                )
            )

    with pytest.raises(OfficialDisclosureQueryError, match="分页超过安全上限"):
        BseDisclosureClient(
            session=FakeSession(), page_size=1, max_pages=1
        ).fetch_announcements("830799", date(2024, 3, 31), date(2024, 5, 1))


def test_official_http_request_retries_transient_failure(monkeypatch):
    class RetrySession:
        def __init__(self):
            self.calls = 0

        def post(self, url, data, timeout):
            self.calls += 1
            if self.calls == 1:
                raise requests.ConnectionError("temporary failure")
            return _FakeResponse({"ok": True})

    monkeypatch.setattr(resolver_mod.time, "sleep", lambda _: None)
    session = RetrySession()

    response = resolver_mod._post_with_retry(
        session,
        "https://example.test",
        data={},
        timeout=1,
        max_retries=1,
        retry_backoff=0,
    )

    assert response.json() == {"ok": True}
    assert session.calls == 2


def test_bse_client_rejects_unknown_response_structure():
    with pytest.raises(OfficialDisclosureQueryError, match="缺少公告列表结构"):
        BseDisclosureClient._extract_content({"unexpected": []})


def test_resolver_routes_market_and_reuses_announcement_cache():
    class FakeClient:
        def __init__(self, disclosure):
            self.disclosure = disclosure
            self.calls = []

        def fetch_announcements(self, symbol, start_date, end_date):
            self.calls.append((symbol, start_date, end_date))
            return [self.disclosure]

    cninfo = FakeClient(
        OfficialDisclosure(date(2024, 4, 26), "公司2024年第一季度报告", "", "cninfo")
    )
    bse = FakeClient(
        OfficialDisclosure(date(2024, 4, 27), "公司2024年第一季度报告", "", "bse")
    )
    resolver = OfficialDisclosureDateResolver(
        cninfo_client=cninfo,
        bse_client=bse,
        today_provider=lambda: date(2026, 8, 17),
    )

    assert (
        resolver.resolve_overdue_report_dates("600519", ["20240331"])["20240331"].source
        == "cninfo"
    )
    assert (
        resolver.resolve_overdue_report_dates("600519", ["20240331"])["20240331"].source
        == "cninfo"
    )
    assert (
        resolver.resolve_overdue_report_dates("920799", ["20240331"])["20240331"].source
        == "bse"
    )
    assert len(cninfo.calls) == 1
    assert len(bse.calls) == 1
