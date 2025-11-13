"""
Helpers for fetching Federal Reserve Board press releases.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup, Tag

FED_NEWS_URL = "https://www.federalreserve.gov/newsevents.htm"
FED_BASE_URL = "https://www.federalreserve.gov"
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

logger = logging.getLogger(__name__)


def _absolute_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if not href.startswith("/"):
        href = f"/{href}"
    return f"{FED_BASE_URL}{href}"


def _request_html(url: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover - network failures
        logger.error("Failed to fetch %s: %s", url, exc)
        return None
    return response.text


def _parse_press_release_listing(html: str, *, limit: int = 5) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict[str, str]] = []

    sections = soup.find_all("div", class_="nePanelBox")
    for section in sections:
        heading = section.find("h4")
        if not heading or "Press Releases" not in heading.get_text():
            continue

        news_items = section.find_all("div", class_="news__item")
        for item in news_items:
            title_elem = item.find("p", class_="news news__title")
            anchor = title_elem.find("a") if title_elem else None
            date_elem = item.find("p", class_="time--sm")
            if not anchor:
                continue

            title = anchor.get_text(strip=True)
            href = anchor.get("href", "").strip()
            url = _absolute_url(href) if href else ""
            published = date_elem.get_text(strip=True) if date_elem else ""

            if not url:
                continue

            results.append(
                {
                    "title": title,
                    "url": url,
                    "publishedText": published,
                }
            )
            if len(results) >= limit:
                break
        if results:
            break

    return results


def _parse_fomc_press_release_listing(year: int, *, limit: int = 10) -> List[Dict[str, str]]:
    """
    Parse the yearly FOMC press release archive for monetary policy statements.
    """

    url = f"{FED_BASE_URL}/newsevents/pressreleases/{year}-press-fomc.htm"
    html = _request_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", class_="col-md-8")
    if not container:
        return []

    rows = container.find_all("div", class_="row")
    results: List[Dict[str, str]] = []
    for row in rows:
        time_tag = row.find("time")
        link_tag = row.find("a")
        if not link_tag:
            continue

        href = (link_tag.get("href") or "").strip()
        url_value = _absolute_url(href) if href else ""
        if not url_value:
            continue

        published = time_tag.get_text(strip=True) if time_tag else ""
        title = link_tag.get_text(strip=True)

        results.append(
            {
                "title": title,
                "url": url_value,
                "publishedText": published,
            }
        )
        if len(results) >= limit:
            break

    return results


def _is_within_share_menu(node: Optional[Tag]) -> bool:
    while node is not None:
        classes = node.get("class") or []
        if any("share" in cls or "dropdown-menu" in cls for cls in classes):
            return True
        node = node.parent
    return False


def _parse_press_release_detail(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find(id="article") or soup.find("main") or soup

    paragraphs: List[str] = []
    for node in article.find_all(["p", "li"]):
        if _is_within_share_menu(node):
            continue
        for anchor in node.select("a.shareLink"):
            anchor.decompose()
        text = node.get_text(" ", strip=True)
        if not text:
            continue
        if text not in paragraphs:
            paragraphs.append(text)

    content = "\n".join(paragraphs)
    raw_text = article.get_text("\n", strip=True)

    return {
        "content": content,
        "rawText": raw_text,
    }


def _parse_published_date(text: str) -> Optional[date]:
    if not text:
        return None
    candidates = [
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y-%m-%d",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def fetch_fed_press_releases(limit: int = 5) -> List[Dict[str, object]]:
    """
    Fetch the latest Federal Reserve and FOMC press releases with full content.
    """
    if limit <= 0:
        return []

    listing_html = _request_html(FED_NEWS_URL)
    general_entries: List[Dict[str, str]] = []
    if listing_html:
        general_entries = _parse_press_release_listing(listing_html, limit=max(limit, 10))
    else:
        logger.warning("Fed press release listing fetch returned no HTML.")

    now = datetime.now()
    fomc_entries: List[Dict[str, str]] = []
    for year in (now.year, now.year - 1):
        fomc_entries.extend(_parse_fomc_press_release_listing(year, limit=max(limit, 10)))
        if len(fomc_entries) >= max(limit, 10):
            break

    combined: Dict[str, Dict[str, str]] = {}
    for item in general_entries + fomc_entries:
        url = (item.get("url") or "").strip()
        if not url or url in combined:
            continue
        combined[url] = item

    def sort_key(entry: Dict[str, str]) -> tuple[date, str]:
        parsed = _parse_published_date(entry.get("publishedText", ""))
        return (parsed or date.min, entry.get("url", ""))

    ordered = sorted(combined.values(), key=sort_key, reverse=True)
    selected = ordered[:limit]

    results: List[Dict[str, object]] = []
    for position, item in enumerate(selected, start=1):
        url = item.get("url")
        detail_html = _request_html(url) if url else None
        detail: Dict[str, str] = {}
        if detail_html:
            detail = _parse_press_release_detail(detail_html)
        else:
            logger.warning("Skipped detail fetch for %s due to missing body.", url)

        published_text = item.get("publishedText", "")
        published_date = _parse_published_date(published_text)

        results.append(
            {
                "title": item.get("title") or "",
                "url": url or "",
                "publishedText": published_text,
                "publishedDate": published_date,
                "content": detail.get("content", ""),
                "rawText": detail.get("rawText", ""),
                "position": position,
            }
        )

    return results


__all__ = [
    "fetch_fed_press_releases",
]
