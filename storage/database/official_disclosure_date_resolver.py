"""交易所官方定期报告公告日期查询。"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import requests

from config.settings import AKSHARE_TIMEOUT
from utils.financial import MarketLabel, get_market_label

CNINFO_TOP_SEARCH_URL = "https://www.cninfo.com.cn/new/information/topSearch/query"
CNINFO_ANNOUNCEMENT_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_STATIC_BASE_URL = "https://static.cninfo.com.cn/"

BSE_ANNOUNCEMENT_URL = (
    "https://www.bse.cn/disclosureInfoController/companyAnnouncement.do"
)
BSE_BASE_URL = "https://www.bse.cn"

REPORT_TYPE_MARKERS: Mapping[str, tuple[str, ...]] = {
    "annual": ("年度报告", "年报"),
    "semiannual": ("半年度报告", "半年报"),
    "q1": ("第一季度报告", "一季度报告"),
    "q3": ("第三季度报告", "三季度报告"),
}
EXCLUDED_TITLE_MARKERS = (
    "摘要",
    "英文",
    "更正",
    "修订",
    "修正",
    "补充",
    "取消",
    "提示性公告",
)
CHINA_TIMEZONE = ZoneInfo("Asia/Shanghai")
CNINFO_CATEGORY = (
    "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh"
)
DEFAULT_HTTP_RETRIES = 2
DEFAULT_HTTP_RETRY_BACKOFF = 0.5
DEFAULT_MAX_PAGE_COUNT = 1000
BSE_NEED_FIELDS = (
    "companyCd",
    "companyName",
    "disclosureTitle",
    "disclosurePostTitle",
    "destFilePath",
    "publishDate",
    "xxfcbj",
    "fileExt",
    "xxzrlx",
)


class OfficialDisclosureQueryError(RuntimeError):
    """官方公告接口请求或响应结构异常。"""


@dataclass(frozen=True)
class OfficialDisclosure:
    """一条官方公告记录。"""

    publish_date: date
    title: str
    url: str
    source: str


class OfficialDisclosureClient(Protocol):
    """官方公告客户端协议。"""

    def fetch_announcements(
        self, symbol: str, start_date: date, end_date: date
    ) -> list[OfficialDisclosure]: ...


def normalize_report_date(value: Any) -> str | None:
    """将报告期或日期值归一化为 YYYYMMDD。"""
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) < 8:
        return None
    normalized = digits[:8]
    try:
        datetime.strptime(normalized, "%Y%m%d")
    except ValueError:
        return None
    return normalized


def parse_date_value(value: Any) -> date | None:
    """解析日期、日期字符串或毫秒时间戳。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.date()
        return value.astimezone(CHINA_TIMEZONE).date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric_text = str(int(value)) if float(value).is_integer() else ""
        if len(numeric_text) == 8 and normalize_report_date(numeric_text):
            return datetime.strptime(numeric_text, "%Y%m%d").date()
        timestamp = float(value)
        if timestamp > 100_000_000_000:
            timestamp /= 1000
        try:
            return (
                datetime.fromtimestamp(timestamp, tz=UTC)
                .astimezone(CHINA_TIMEZONE)
                .date()
            )
        except (OverflowError, OSError, ValueError):
            return None
    normalized = normalize_report_date(value)
    if normalized is None:
        return None
    return datetime.strptime(normalized, "%Y%m%d").date()


def get_report_type(report_date: str) -> str | None:
    """根据报告期月日返回报告类型。"""
    normalized = normalize_report_date(report_date)
    if normalized is None:
        return None
    return {
        "0331": "q1",
        "0630": "semiannual",
        "0930": "q3",
        "1231": "annual",
    }.get(normalized[4:])


def get_disclosure_deadline(report_date: str) -> date | None:
    """按定期报告规则计算法定披露截止日。"""
    normalized = normalize_report_date(report_date)
    report_type = get_report_type(normalized or "")
    if normalized is None or report_type is None:
        return None
    year = int(normalized[:4])
    if report_type == "q1":
        return date(year, 4, 30)
    if report_type == "semiannual":
        return date(year, 8, 31)
    if report_type == "q3":
        return date(year, 10, 31)
    return date(year + 1, 4, 30)


def is_publish_date_overdue(report_date: str, publish_date: str | date) -> bool:
    """判断公告日期是否晚于报告期对应的法定截止日。"""
    deadline = get_disclosure_deadline(report_date)
    actual_date = parse_date_value(publish_date)
    return deadline is not None and actual_date is not None and actual_date > deadline


def title_matches_report(title: str, report_date: str) -> bool:
    """判断公告标题是否为指定报告期的原始定期报告。"""
    normalized = normalize_report_date(report_date)
    report_type = get_report_type(normalized or "")
    if not title or normalized is None or report_type is None:
        return False
    if normalized[:4] not in title:
        return False
    if any(marker in title for marker in EXCLUDED_TITLE_MARKERS):
        return False
    return any(marker in title for marker in REPORT_TYPE_MARKERS[report_type])


def select_first_disclosure(
    announcements: Sequence[OfficialDisclosure], report_date: str
) -> OfficialDisclosure | None:
    """从公告列表中选择指定报告期的最早原始报告。"""
    candidates = [
        item for item in announcements if title_matches_report(item.title, report_date)
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda item: (item.publish_date, item.title, item.url))


def _absolute_url(base_url: str, path: Any) -> str:
    value = str(path or "")
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return f"{base_url.rstrip('/')}/{value.lstrip('/')}"


def _response_json(response: Any, endpoint: str) -> Any:
    try:
        response.raise_for_status()
        return response.json()
    except (AttributeError, ValueError, requests.RequestException) as exc:
        raise OfficialDisclosureQueryError(
            f"官方公告接口响应异常: {endpoint}: {exc}"
        ) from exc


def _post_with_retry(
    session: requests.Session,
    endpoint: str,
    *,
    data: Mapping[str, Any],
    timeout: int,
    max_retries: int,
    retry_backoff: float,
) -> Any:
    """请求官方接口并对瞬时 HTTP/网络错误进行指数退避重试。"""
    if max_retries < 0:
        raise ValueError("max_retries 不能为负数")
    if retry_backoff < 0:
        raise ValueError("retry_backoff 不能为负数")

    for attempt in range(max_retries + 1):
        try:
            response = session.post(endpoint, data=data, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt >= max_retries:
                raise
            time.sleep(retry_backoff * (2**attempt))

    raise AssertionError("不可达的 HTTP 重试分支")


class CninfoDisclosureClient:
    """巨潮资讯官方公告客户端，覆盖沪深市场。"""

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: int = AKSHARE_TIMEOUT,
        page_size: int = 100,
        max_pages: int = DEFAULT_MAX_PAGE_COUNT,
        max_retries: int = DEFAULT_HTTP_RETRIES,
        retry_backoff: float = DEFAULT_HTTP_RETRY_BACKOFF,
    ) -> None:
        if page_size <= 0 or max_pages <= 0:
            raise ValueError("page_size 和 max_pages 必须为正数")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.page_size = page_size
        self.max_pages = max_pages
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.cninfo.com.cn/new/disclosure/stock",
            }
        )

    def _resolve_org_id(self, symbol: str) -> tuple[str, str]:
        response = _post_with_retry(
            self.session,
            CNINFO_TOP_SEARCH_URL,
            data={"keyWord": symbol},
            timeout=self.timeout,
            max_retries=self.max_retries,
            retry_backoff=self.retry_backoff,
        )
        payload = _response_json(response, CNINFO_TOP_SEARCH_URL)
        if not isinstance(payload, list):
            raise OfficialDisclosureQueryError(f"巨潮股票映射响应不是列表: {symbol}")
        match = next(
            (
                item
                for item in payload
                if isinstance(item, dict) and str(item.get("code")) == symbol
            ),
            None,
        )
        if match is None or not match.get("orgId"):
            raise OfficialDisclosureQueryError(f"巨潮未找到股票组织机构代码: {symbol}")
        return str(match["orgId"]), ("sse" if symbol.startswith("6") else "szse")

    @staticmethod
    def _parse_announcement(item: dict[str, Any]) -> OfficialDisclosure | None:
        publish_date = parse_date_value(
            item.get("announcementTime") or item.get("announcementDate")
        )
        if publish_date is None:
            return None
        return OfficialDisclosure(
            publish_date=publish_date,
            title=str(item.get("announcementTitle") or "").strip(),
            url=_absolute_url(CNINFO_STATIC_BASE_URL, item.get("adjunctUrl")),
            source="cninfo",
        )

    def fetch_announcements(
        self, symbol: str, start_date: date, end_date: date
    ) -> list[OfficialDisclosure]:
        org_id, column = self._resolve_org_id(symbol)
        announcements: list[OfficialDisclosure] = []
        seen_pages: set[tuple[str, ...]] = set()
        for page_num in range(1, self.max_pages + 1):
            response = _post_with_retry(
                self.session,
                CNINFO_ANNOUNCEMENT_URL,
                data={
                    "stock": f"{symbol},{org_id}",
                    "searchkey": "",
                    "plate": "",
                    "category": CNINFO_CATEGORY,
                    "trade": "",
                    "column": column,
                    "pageNum": str(page_num),
                    "pageSize": str(self.page_size),
                    "tabName": "fulltext",
                    "sortName": "",
                    "sortType": "",
                    "limit": "",
                    "showTitle": "",
                    "seDate": f"{start_date:%Y-%m-%d}~{end_date:%Y-%m-%d}",
                    "isHLtitle": "true",
                },
                timeout=self.timeout,
                max_retries=self.max_retries,
                retry_backoff=self.retry_backoff,
            )
            payload = _response_json(response, CNINFO_ANNOUNCEMENT_URL)
            if not isinstance(payload, dict):
                raise OfficialDisclosureQueryError(f"巨潮公告响应不是对象: {symbol}")
            if "announcements" not in payload:
                raise OfficialDisclosureQueryError(
                    f"巨潮公告响应缺少公告列表: {symbol}"
                )
            items = payload["announcements"]
            if not isinstance(items, list):
                raise OfficialDisclosureQueryError(f"巨潮公告字段不是列表: {symbol}")
            parsed = [
                announcement
                for item in items
                if isinstance(item, dict)
                for announcement in [self._parse_announcement(item)]
                if announcement is not None
            ]
            announcements.extend(parsed)
            page_signature = tuple(
                f"{item.get('announcementTime')}|{item.get('announcementTitle')}|"
                f"{item.get('adjunctUrl')}"
                for item in items
                if isinstance(item, dict)
            )
            if not items or page_signature in seen_pages:
                break
            seen_pages.add(page_signature)
            total = _to_int(
                payload.get("totalRecord") or payload.get("totalAnnouncement")
            )
            if total is not None and page_num * self.page_size >= total:
                break
            if total is None and len(items) < self.page_size:
                break
        else:
            raise OfficialDisclosureQueryError(
                f"巨潮公告分页超过安全上限 {self.max_pages}: {symbol}"
            )
        return announcements


def _decode_jsonp(payload: str) -> Any:
    """解析北交所返回的 callback([...]) / null([...]) 响应。"""
    text = payload.strip()
    if not text:
        raise OfficialDisclosureQueryError("北交所返回空响应")
    if text.startswith("{") or text.startswith("["):
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise OfficialDisclosureQueryError("北交所 JSON 响应解析失败") from exc
    start = text.find("(")
    end = text.rfind(")")
    if start < 0 or end <= start:
        raise OfficialDisclosureQueryError("北交所 JSONP 响应格式异常")
    try:
        return json.loads(text[start + 1 : end])
    except json.JSONDecodeError as exc:
        raise OfficialDisclosureQueryError("北交所 JSONP 内容解析失败") from exc


def _normalize_bse_symbol(symbol: str) -> str:
    if symbol.startswith(("4", "8")):
        return f"920{symbol[-3:]}"
    return symbol


class BseDisclosureClient:
    """北京证券交易所官方公告客户端。"""

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: int = AKSHARE_TIMEOUT,
        page_size: int = 20,
        max_pages: int = DEFAULT_MAX_PAGE_COUNT,
        max_retries: int = DEFAULT_HTTP_RETRIES,
        retry_backoff: float = DEFAULT_HTTP_RETRY_BACKOFF,
    ) -> None:
        if page_size <= 0 or max_pages <= 0:
            raise ValueError("page_size 和 max_pages 必须为正数")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.page_size = page_size
        self.max_pages = max_pages
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.bse.cn/disclosure/announcement.html",
                "Origin": "https://www.bse.cn",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

    @staticmethod
    def _extract_content(payload: Any) -> tuple[list[dict[str, Any]], int | None]:
        if isinstance(payload, dict) and any(
            payload.get(key) not in (None, "", 0, "0", False)
            for key in ("error", "errorCode", "errorMessage", "message")
        ):
            raise OfficialDisclosureQueryError("北交所公告接口返回业务错误")
        if payload == []:
            return [], 0
        if isinstance(payload, list):
            blocks = payload
        elif isinstance(payload, dict):
            blocks = [payload]
        else:
            raise OfficialDisclosureQueryError("北交所公告响应顶层结构异常")
        for block in blocks:
            if not isinstance(block, dict):
                continue
            list_info = block.get("listInfo")
            if isinstance(list_info, dict):
                if "content" not in list_info:
                    raise OfficialDisclosureQueryError(
                        "北交所公告响应缺少 listInfo.content"
                    )
                content = list_info["content"]
                total = list_info.get("totalElements")
                if not isinstance(content, list):
                    raise OfficialDisclosureQueryError(
                        "北交所公告 listInfo.content 不是列表"
                    )
                return content, _to_int(total)
            data = block.get("data")
            if isinstance(data, dict):
                content_key = "content" if "content" in data else "disclosures"
                if content_key not in data:
                    raise OfficialDisclosureQueryError(
                        "北交所公告响应缺少 data.content"
                    )
                content = data[content_key]
                total = data.get("totalElements")
                if not isinstance(content, list):
                    raise OfficialDisclosureQueryError(
                        "北交所公告 data.content 不是列表"
                    )
                return content, _to_int(total)
        raise OfficialDisclosureQueryError("北交所公告响应缺少公告列表结构")

    @staticmethod
    def _parse_announcement(item: dict[str, Any]) -> OfficialDisclosure | None:
        publish_date = parse_date_value(item.get("publishDate"))
        if publish_date is None:
            return None
        title = "".join(
            str(item.get(key) or "")
            for key in ("disclosureTitle", "disclosurePostTitle")
        ).strip()
        return OfficialDisclosure(
            publish_date=publish_date,
            title=title,
            url=_absolute_url(BSE_BASE_URL, item.get("destFilePath")),
            source="bse",
        )

    def _request_page(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        page: int,
        layers: list[str],
    ) -> tuple[list[dict[str, Any]], int | None]:
        payload: dict[str, Any] = {
            "disclosureType": [],
            "disclosureSubtype": [],
            "page": str(page),
            "pageSize": str(self.page_size),
            "pageNum": str(page),
            "companyCd": symbol,
            "isNewThree": "1",
            "startTime": f"{start_date:%Y-%m-%d}",
            "endTime": f"{end_date:%Y-%m-%d}",
            "keyword": "",
            "xxfcbj": layers,
            "hyType": [],
            "needFields": list(BSE_NEED_FIELDS),
        }
        response = _post_with_retry(
            self.session,
            BSE_ANNOUNCEMENT_URL,
            data=payload,
            timeout=self.timeout,
            max_retries=self.max_retries,
            retry_backoff=self.retry_backoff,
        )
        try:
            payload_json = _decode_jsonp(response.text)
        except AttributeError as exc:
            raise OfficialDisclosureQueryError(
                f"北交所公告接口请求失败: {symbol}: {exc}"
            ) from exc
        return self._extract_content(payload_json)

    def fetch_announcements(
        self, symbol: str, start_date: date, end_date: date
    ) -> list[OfficialDisclosure]:
        codes = list(dict.fromkeys((_normalize_bse_symbol(symbol), symbol)))
        layer_options = (["0", "1", "2"], ["2"], ["1"], ["0"], [])
        for query_symbol in codes:
            for layers in layer_options:
                all_items: list[dict[str, Any]] = []
                seen_pages: set[tuple[str, ...]] = set()
                for page in range(0, self.max_pages):
                    items, total = self._request_page(
                        query_symbol, start_date, end_date, page, list(layers)
                    )
                    all_items.extend(item for item in items if isinstance(item, dict))
                    page_signature = tuple(
                        f"{item.get('publishDate')}|{item.get('disclosureTitle')}|"
                        f"{item.get('destFilePath')}"
                        for item in items
                        if isinstance(item, dict)
                    )
                    if not items or page_signature in seen_pages:
                        break
                    seen_pages.add(page_signature)
                    if total is not None and (page + 1) * self.page_size >= total:
                        break
                    if total is None and len(items) < self.page_size:
                        break
                else:
                    raise OfficialDisclosureQueryError(
                        f"北交所公告分页超过安全上限 {self.max_pages}: {symbol}"
                    )
                parsed = [
                    announcement
                    for item in all_items
                    for announcement in [self._parse_announcement(item)]
                    if announcement is not None
                ]
                if parsed:
                    return parsed
        return []


def _to_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class OfficialDisclosureDateResolver:
    """按市场路由官方公告查询，并按报告期选择首发日期。"""

    def __init__(
        self,
        cninfo_client: OfficialDisclosureClient | None = None,
        bse_client: OfficialDisclosureClient | None = None,
        today_provider: Any = date.today,
    ) -> None:
        self.cninfo_client = cninfo_client or CninfoDisclosureClient()
        self.bse_client = bse_client or BseDisclosureClient()
        self.today_provider = today_provider
        self._announcement_cache: dict[
            tuple[str, date, date], list[OfficialDisclosure]
        ] = {}

    def _client_for_symbol(self, symbol: str) -> OfficialDisclosureClient:
        return (
            self.bse_client
            if get_market_label(symbol) is MarketLabel.BJ
            else self.cninfo_client
        )

    def resolve_overdue_report_dates(
        self,
        symbol: str,
        report_dates: Sequence[str],
        end_date: date | None = None,
    ) -> dict[str, OfficialDisclosure]:
        normalized_dates = sorted(
            {
                normalized
                for report_date in report_dates
                if (normalized := normalize_report_date(report_date)) is not None
            }
        )
        if not normalized_dates:
            return {}

        period_dates = [
            datetime.strptime(report_date, "%Y%m%d").date()
            for report_date in normalized_dates
        ]
        start_date = min(period_dates)
        final_date = end_date or self.today_provider()
        deadlines = [
            deadline
            for report_date in normalized_dates
            if (deadline := get_disclosure_deadline(report_date)) is not None
        ]
        if deadlines:
            final_date = max(final_date, max(deadlines))

        cache_key = (symbol, start_date, final_date)
        if cache_key not in self._announcement_cache:
            client = self._client_for_symbol(symbol)
            self._announcement_cache[cache_key] = client.fetch_announcements(
                symbol, start_date, final_date
            )
        announcements = self._announcement_cache[cache_key]
        return {
            report_date: selected
            for report_date in normalized_dates
            if (selected := select_first_disclosure(announcements, report_date))
            is not None
        }
