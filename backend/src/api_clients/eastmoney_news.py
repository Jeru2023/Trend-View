"""
Helpers to scrape detailed finance breakfast articles from Eastmoney.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Final, Iterable, Optional
from urllib.parse import urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS: Final[dict[str, str]] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

START_KEYWORDS: Final[tuple[str, ...]] = ("每日精选", "每日精選")
END_KEYWORDS: Final[tuple[str, ...]] = ("财经日历", "交易日历", "财经日曆", "交易日曆")


@dataclass(frozen=True)
class EastmoneyNewsDetail:
    """Structured detail result returned by :func:`fetch_eastmoney_detail`."""

    title: Optional[str]
    content: Optional[str]
    url: str


def fetch_eastmoney_detail(url: str, *, timeout: float = 2.0) -> EastmoneyNewsDetail:
    """
    Fetch detailed news content from an Eastmoney finance article page.

    Args:
        url: Target article URL.
        timeout: Requests timeout in seconds.

    Returns:
        EastmoneyNewsDetail containing the parsed title and article content.
    """
    attempt_urls: list[str] = [url]
    fallback_url = _http_fallback_url(url)
    if fallback_url:
        attempt_urls.append(fallback_url)

    response = None
    final_url = url

    for candidate in attempt_urls:
        try:
            response = requests.get(candidate, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            final_url = candidate
            break
        except requests.exceptions.SSLError as exc:  # pragma: no cover - network errors
            logger.warning("SSL error fetching Eastmoney article %s: %s", candidate, exc)
        except requests.RequestException as exc:  # pragma: no cover - network errors
            logger.warning("Failed to fetch Eastmoney article %s: %s", candidate, exc)
        response = None

    if response is None:
        return EastmoneyNewsDetail(title=None, content=None, url=url)

    # Eastmoney pages mostly use UTF-8; fall back to chardet if available.
    detected_encoding = getattr(response, "apparent_encoding", None)
    response.encoding = detected_encoding or response.encoding or "utf-8"

    soup = BeautifulSoup(response.text, "html.parser")
    title_element = soup.find("h1")
    title = title_element.get_text(strip=True) if title_element else None

    content = _extract_from_dom(soup)
    if not content:
        full_text = soup.get_text(separator="\n")
        content = _extract_content_by_keywords(full_text)

    if not content:
        wap_url = _to_wap_url(url)
        if wap_url:
            try:
                wap_response = requests.get(wap_url, headers=HEADERS, timeout=timeout)
                wap_response.raise_for_status()
            except requests.RequestException as exc:  # pragma: no cover
                logger.info("Failed to fetch WAP article %s: %s", wap_url, exc)
            else:
                wap_response.encoding = getattr(wap_response, "apparent_encoding", None) or wap_response.encoding or "utf-8"
                wap_soup = BeautifulSoup(wap_response.text, "html.parser")
                content = _extract_from_dom(wap_soup)
                if not content:
                    full_text = wap_soup.get_text(separator="\n")
                    content = _extract_content_by_keywords(full_text)

    if not content:
        logger.info("Eastmoney article detail extraction returned empty content: url=%s", url)

    return EastmoneyNewsDetail(
        title=title,
        content=content,
        url=url,
    )


def _extract_content_by_keywords(full_text: str) -> Optional[str]:
    """Extract the finance breakfast body by scanning for known keyword anchors."""
    if not full_text:
        return None

    start_index = -1
    for keyword in START_KEYWORDS:
        start_index = full_text.find(keyword)
        if start_index != -1:
            break

    if start_index == -1:
        logger.debug("Eastmoney detail content missing start keyword.")
        return None

    end_index = -1
    for keyword in END_KEYWORDS:
        end_index = full_text.find(keyword, start_index)
        if end_index != -1:
            break

    slice_end = end_index if end_index != -1 else start_index + 5000
    raw_content = full_text[start_index:slice_end]
    cleaned = raw_content.strip()
    return _clean_text(cleaned)


def _extract_from_dom(soup: BeautifulSoup) -> Optional[str]:
    """Attempt to extract the article body by traversing known DOM containers."""
    containers: Iterable[Optional[BeautifulSoup]] = (
        soup.find(id="ContentBody"),
        soup.find("div", class_="content-body"),
        soup.find("div", class_="article-content"),
    )

    for container in containers:
        if not container:
            continue
        paragraphs = container.find_all(["p", "li"])
        texts: list[str] = []
        if paragraphs:
            for node in paragraphs:
                text = node.get_text(" ", strip=True)
                text = _clean_text(text)
                if text:
                    texts.append(text)
        else:
            for snippet in container.stripped_strings:
                text = _clean_text(snippet)
                if text:
                    texts.append(text)
        if texts:
            return "\n\n".join(texts)
    return None


def _to_wap_url(url: str) -> Optional[str]:
    if not url:
        return None
    if "wap.eastmoney.com" in url:
        return url
    match = re.search(r"/a/(\d+)\.html", url)
    if match:
        return f"https://wap.eastmoney.com/a/{match.group(1)}.html"
    return None


def _clean_text(text: str) -> Optional[str]:
    if not text:
        return None
    cleaned = text.replace("\u3000", " ").replace("\xa0", " ")
    cleaned = re.sub(r"\s+\n", "\n", cleaned)
    cleaned = re.sub(r"\n\s+", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = cleaned.strip()
    cleaned = re.sub(r"(财经日历|财经日曆)\s*$", "", cleaned)
    cleaned = cleaned.strip()
    return cleaned or None


__all__ = ["EastmoneyNewsDetail", "fetch_eastmoney_detail"]


def _http_fallback_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if parsed.scheme.lower() == "https":
        fallback = parsed._replace(scheme="http")
        return urlunparse(fallback)
    return None
