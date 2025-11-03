"""
Quick smoke test for pulling policy and market-moving news from key regulators.

The script now grabs the *entire first page* from each source, extracts any
available publication date, and then sorts the results so that the “latest”
items truly reflect the newest documents.  It still only prints a concise
subset (default：5 条) for readability, but the internal list contains every
headline discovered on the page.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5",
}
MAX_ITEMS_TO_DISPLAY = 5


@dataclass
class PolicyItem:
    source: str
    title: str
    url: str
    date_text: str | None = None
    date_value: Optional[date] = None
    extra: str | None = None


def fetch_json(url: str) -> list[dict]:
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Failed to decode JSON from {url}") from exc


def fetch_html(url: str) -> BeautifulSoup:
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()
    encoding = response.apparent_encoding or response.encoding or "utf-8"
    response.encoding = encoding
    return BeautifulSoup(response.text, "html.parser")


def parse_date(text: str | None, *formats: str) -> Optional[date]:
    if not text:
        return None
    cleaned = text.strip()
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def parse_month_day(text: str | None) -> Optional[date]:
    """Interpret strings like '10-16' as (current year) month/day with rollover logic."""
    if not text:
        return None
    try:
        month, day = (int(part) for part in text.strip().split("-"))
    except ValueError:
        return None

    today = datetime.today().date()
    candidate = date(today.year, month, day)
    # If the inferred date is > ~1 week in the future, treat it as previous year.
    if candidate - today > timedelta(days=7):
        candidate = date(today.year - 1, month, day)
    return candidate


def deduplicate(items: Iterable[PolicyItem]) -> List[PolicyItem]:
    seen: set[tuple[str, str]] = set()
    unique: List[PolicyItem] = []
    for item in items:
        if not item.title or not item.url:
            continue
        key = (item.title, item.url)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def sort_items(items: List[PolicyItem]) -> List[PolicyItem]:
    dated = [item for item in items if item.date_value is not None]
    undated = [item for item in items if item.date_value is None]
    dated.sort(key=lambda item: item.date_value, reverse=True)  # newest first
    return dated + undated


def summarise(items: List[PolicyItem], limit: int) -> List[PolicyItem]:
    if limit <= 0 or len(items) <= limit:
        return items
    return items[:limit]


def fetch_gov_policies() -> List[PolicyItem]:
    """国务院政策库 JSON 源。"""
    payload = fetch_json("https://www.gov.cn/pushinfo/v150203/pushinfo.json")
    items = [
        PolicyItem(
            source="国务院",
            title=entry.get("title", "").strip(),
            url=entry.get("link", "").strip(),
            date_text=entry.get("pubDate", ""),
            date_value=parse_date(entry.get("pubDate", ""), "%Y-%m-%d"),
        )
        for entry in payload
        if entry.get("title") and entry.get("link")
    ]
    return sort_items(deduplicate(items))


def fetch_pbc_regulations() -> List[PolicyItem]:
    """央行“部门规章”栏目。"""
    soup = fetch_html("http://www.pbc.gov.cn/tiaofasi/144941/144957/index.html")
    container = soup.find("div", id="r_con")
    if not container:
        return []

    items: List[PolicyItem] = []
    for row in container.select("table tr td font a"):
        anchor = row
        span = anchor.find_parent("td").find_all("span")
        date_str = span[-1].get_text(strip=True) if span else ""
        items.append(
            PolicyItem(
                source="人民银行",
                title=anchor.get_text(strip=True),
                url=urljoin("http://www.pbc.gov.cn", anchor.get("href", "")),
                date_text=date_str,
                date_value=parse_date(date_str, "%Y-%m-%d"),
            )
        )
    return sort_items(deduplicate(items))


def fetch_mof_policies() -> List[PolicyItem]:
    """财政部政策发布栏目。"""
    soup = fetch_html("http://www.mof.gov.cn/zhengwuxinxi/zhengcefabu/")
    container = soup.find("div", class_="mainboxerji")
    if not container:
        return []

    items: List[PolicyItem] = []
    for li in container.select("ul.xwfb_listbox li"):
        anchor = li.find("a")
        if not anchor:
            continue
        span = li.find("span")
        date_str = span.get_text(strip=True) if span else ""
        items.append(
            PolicyItem(
                source="财政部",
                title=anchor.get_text(strip=True),
                url=urljoin("http://www.mof.gov.cn/zhengwuxinxi/zhengcefabu/", anchor.get("href", "")),
                date_text=date_str,
                date_value=parse_date(date_str, "%Y-%m-%d"),
            )
        )
    return sort_items(deduplicate(items))


def fetch_ndrc_releases() -> List[PolicyItem]:
    """发改委文件库（发展改革委令板块）。"""
    soup = fetch_html("https://www.ndrc.gov.cn/xxgk/zcfb/fzggwl/")
    items: List[PolicyItem] = []
    for li in soup.select("ul.u-list li"):
        anchor = li.find("a")
        span = li.find("span")
        if not anchor or not span:
            continue  # 过滤掉“解读”之类无日期的条目
        date_str = span.get_text(strip=True)
        items.append(
            PolicyItem(
                source="发改委",
                title=anchor.get_text(strip=True),
                url=urljoin("https://www.ndrc.gov.cn/xxgk/zcfb/fzggwl/", anchor.get("href", "")),
                date_text=date_str,
                date_value=parse_date(date_str, "%Y/%m/%d", "%Y-%m-%d"),
            )
        )
    return sort_items(deduplicate(items))


def fetch_csrc_documents() -> List[PolicyItem]:
    """证监会规范性文件/公告目录页（带行政复议、处罚、禁入等栏目）。"""
    soup = fetch_html("http://www.csrc.gov.cn/csrc/c101933/zhengce_list.shtml")
    items: List[PolicyItem] = []
    for li in soup.select("div.tab-list ul li"):
        anchor = li.find("a")
        span = li.find("span", class_="time")
        if not anchor or not span:
            continue
        href = anchor.get("href", "")
        if not any(identifier in href for identifier in ("c101927", "c101928", "c101933")):
            continue
        date_str = span.get_text(strip=True)
        items.append(
            PolicyItem(
                source="证监会",
                title=anchor.get_text(strip=True),
                url=urljoin("http://www.csrc.gov.cn", href),
                date_text=date_str,
                date_value=parse_month_day(date_str),
            )
        )
    return sort_items(deduplicate(items))


def run(label: str, fetcher: Callable[[], List[PolicyItem]], limit: int = MAX_ITEMS_TO_DISPLAY) -> None:
    try:
        items = fetcher()
    except Exception as exc:  # pragma: no cover - diagnostic
        print(f"[{label}] 拉取失败: {exc}")
        return

    if not items:
        print(f"[{label}] 未获取到任何结果")
        return

    display = summarise(items, limit)
    print(f"[{label}] 共抓取 {len(items)} 条（显示 {len(display)} 条）：")
    for item in display:
        pieces = []
        if item.date_text:
            pieces.append(item.date_text)
        if item.extra:
            pieces.append(item.extra)
        meta = f"（{'，'.join(pieces)}）" if pieces else ""
        print(f"  - {item.title}{meta}\n    {item.url}")


def main() -> None:
    run("国务院政策", fetch_gov_policies)
    run("人民银行规章", fetch_pbc_regulations)
    run("财政部政策发布", fetch_mof_policies)
    run("发改委令", fetch_ndrc_releases)
    run("证监会文件", fetch_csrc_documents)


if __name__ == "__main__":
    main()
