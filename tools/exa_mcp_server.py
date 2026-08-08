"""Exa 搜索 MCP 服务器：以本地 stdio 进程向 opencode 提供 Exa 搜索能力。

支持多 API Key 轮换（环境变量 EXA_API_KEYS 逗号分隔，或 config/exa_keys.json）。
提供三个 MCP 工具：
- web_search_exa: 快速网络搜索（含高亮摘要）
- web_fetch_exa: 按 URL 抓取网页正文为 markdown
- web_search_advanced_exa: 高级搜索（品类/域名/日期过滤）

运行方式（由 .opencode/opencode.json 中的 local MCP 配置拉起）：
    uv run tools/exa_mcp_server.py
"""

from __future__ import annotations

import itertools
import json
import os
import threading
from pathlib import Path
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

EXA_SEARCH_URL = "https://api.exa.ai/search"
EXA_CONTENTS_URL = "https://api.exa.ai/contents"
FALLBACK_KEYS_FILE = Path(__file__).resolve().parent.parent / "config" / "exa_keys.json"

mcp = FastMCP(
    "exa",
    instructions=(
        "使用 Exa 搜索引擎检索网络信息。web_search_exa 适合常规查询，"
        "web_search_advanced_exa 支持按品类/域名/发布时间过滤，"
        "web_fetch_exa 用于抓取指定 URL 的正文内容。"
    ),
)


def _load_api_keys() -> list[str]:
    raw = os.environ.get("EXA_API_KEYS", "").strip()
    if raw:
        return [k.strip() for k in raw.split(",") if k.strip()]

    if FALLBACK_KEYS_FILE.exists():
        try:
            data = json.loads(FALLBACK_KEYS_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"无法解析 {FALLBACK_KEYS_FILE}: {exc}") from exc
        keys = data.get("keys", [])
        if keys:
            return [k.strip() for k in keys if k.strip()]

    raise RuntimeError(
        "未找到 Exa API Key：请设置环境变量 EXA_API_KEYS（逗号分隔多个 key），"
        '或在 config/exa_keys.json 中提供 {"keys": ["..."]} 格式的配置。'
    )


class ExaClient:
    """封装 Exa REST API，实现多 key 轮换与 429 自动切换重试。"""

    def __init__(self) -> None:
        self._keys = _load_api_keys()
        self._cycle = itertools.cycle(self._keys)
        self._lock = threading.Lock()

    def _next_key(self) -> str:
        with self._lock:
            return next(self._cycle)

    def _request(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for _ in range(len(self._keys)):
            key = self._next_key()
            try:
                resp = httpx.post(
                    url,
                    headers={"x-api-key": key, "content-type": "application/json"},
                    json=payload,
                    timeout=60.0,
                )
            except httpx.HTTPError as exc:
                last_error = exc
                continue
            if resp.status_code == 429:
                last_error = RuntimeError("key 触限流 (429)，切换下一 key 重试")
                continue
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Exa API 返回 {resp.status_code}: {resp.text[:500]}"
                )
            return resp.json()
        raise RuntimeError(f"全部 {len(self._keys)} 个 key 请求均失败: {last_error}")

    def search(self, query: str, num_results: int, **kwargs: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "query": query,
            "numResults": num_results,
            "type": kwargs.get("type", "auto"),
            "contents": kwargs.get(
                "contents", {"highlights": {"maxHighlights": 3, "numSentences": 3}}
            ),
        }
        for field in (
            "category",
            "include_domains",
            "exclude_domains",
            "start_published_date",
            "end_published_date",
        ):
            value = kwargs.get(field)
            if value:
                payload[field] = value
        return self._request(EXA_SEARCH_URL, payload)

    def fetch(self, urls: list[str]) -> dict[str, Any]:
        payload = {
            "urls": urls,
            "text": {"includeHtml": False},
            "livecrawl": "fallback",
        }
        return self._request(EXA_CONTENTS_URL, payload)


_client: ExaClient | None = None


def _get_client() -> ExaClient:
    global _client
    if _client is None:
        _client = ExaClient()
    return _client


def _format_search_result(data: dict[str, Any]) -> str:
    results = data.get("results", [])
    if not results:
        return "未找到相关结果。"
    lines = []
    for i, item in enumerate(results, 1):
        lines.append(
            f"{i}. {item.get('title', '(无标题)')}\n"
            f"   URL: {item.get('url', '(无链接)')}"
        )
        published = item.get("publishedDate")
        if published:
            lines.append(f"   发布日期: {published}")
        highlights = item.get("highlights") or []
        if highlights:
            lines.append(f"   摘要: {' | '.join(highlights[:2])}")
    return "\n".join(lines)


@mcp.tool(
    name="web_search_exa",
    title="Exa 网络搜索",
    description=(
        "通过 Exa 搜索引擎检索网络信息，返回带高亮摘要的结果列表（标题/URL/发布日期）。"
        "适合常规主题查询、新闻检索、行业信息收集。"
    ),
)
def web_search_exa(query: str, num_results: int = 8) -> str:
    """执行一次 Exa 网络搜索。

    Args:
        query: 搜索查询词（支持自然语言语义检索，中英文均可）。
        num_results: 返回结果数量，默认 8，最大 25。
    """
    if num_results < 1 or num_results > 25:
        raise ValueError("num_results 必须在 1-25 之间")
    data = _get_client().search(query, num_results)
    return _format_search_result(data)


@mcp.tool(
    name="web_fetch_exa",
    title="Exa 网页抓取",
    description="抓取一个或多个网页 URL 的正文内容，返回纯文本（便于 LLM 直接阅读，省 token）。",
)
def web_fetch_exa(urls: list[str]) -> str:
    """按 URL 抓取网页正文。

    Args:
        urls: 需要抓取的网页 URL 列表（一次最多 10 个）。
    """
    if not urls:
        raise ValueError("urls 不能为空")
    if len(urls) > 10:
        raise ValueError("单次最多抓取 10 个 URL")
    data = _get_client().fetch(urls)
    results = data.get("results", [])
    if not results:
        return "未能抓取到任何页面内容。"
    lines = []
    for item in results:
        title = item.get("title", "(无标题)")
        url = item.get("url", "(无链接)")
        text = item.get("text", "")
        if not text.strip():
            text = "(页面无有效文本内容)"
        lines.append(f"## {title}\n来源: {url}\n\n{text.strip()[:20000]}")
    return "\n\n---\n\n".join(lines)


@mcp.tool(
    name="web_search_advanced_exa",
    title="Exa 高级搜索",
    description=(
        "Exa 高级搜索：支持按品类（news/company/paper 等）、指定域名包含/排除、"
        "发布时间区间过滤的精细化检索。适合限定来源或时效的研究场景。"
    ),
)
def web_search_advanced_exa(
    query: str,
    num_results: int = 8,
    category: str | None = None,
    include_domains: list[str] | None = None,
    exclude_domains: list[str] | None = None,
    start_published_date: str | None = None,
    end_published_date: str | None = None,
) -> str:
    """执行一次 Exa 高级搜索（含过滤条件）。

    Args:
        query: 搜索查询词。
        num_results: 返回结果数量，默认 8，最大 25。
        category: 结果品类过滤，可选 general / news / company / paper / financial_report 等。
        include_domains: 仅返回这些域名下的结果（如 ["exa.ai"]，支持路径前缀如 "exa.ai/blog"）。
        exclude_domains: 排除这些域名下的结果。
        start_published_date: 结果最早发布日期（ISO 格式，如 "2026-01-01"）。
        end_published_date: 结果最晚发布日期（ISO 格式，如 "2026-07-31"）。
    """
    if num_results < 1 or num_results > 25:
        raise ValueError("num_results 必须在 1-25 之间")
    data = _get_client().search(
        query,
        num_results,
        category=category,
        include_domains=include_domains,
        exclude_domains=exclude_domains,
        start_published_date=start_published_date,
        end_published_date=end_published_date,
    )
    return _format_search_result(data)


if __name__ == "__main__":
    mcp.run(transport="stdio")
