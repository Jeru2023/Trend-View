#!/usr/bin/env python3
"""
Scrape research reports from HuiYunYan search results.

Usage:
    python scripts/scrape_huiyunyan_reports.py --keyword 博俊科技
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import List
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://www.huiyunyan.com/search.html?i={keyword}"


@dataclass
class Report:
    title: str
    link: str
    published_at: str
    agency: str
    author: str
    rating: str
    pages: str


def fetch_reports(keyword: str, limit: int = 20) -> List[Report]:
    encoded = quote_plus(keyword)
    url = BASE_URL.format(keyword=encoded)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Referer": "http://www.huiyunyan.com/",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    soup = BeautifulSoup(resp.text, "html.parser")
    reports: List[Report] = []
    for title_tag in soup.select("h3 a"):
        href = title_tag.get("href", "")
        if "/doc-" not in href:
            continue
        title = title_tag.get_text(strip=True)
        link = urljoin(url, href)
        meta = title_tag.find_parent("h3")
        info_block = None
        if meta:
            info_block = meta.find_next("div", class_="user-tip")
        title = title_tag.get_text(strip=True)
        link = urljoin(url, href)
        published_at = agency = author = rating = pages = ""
        if info_block:
            spans = info_block.select("span")
            if spans:
                published_at = spans[0].get_text(strip=True)
                if len(spans) > 1:
                    agency = spans[1].get_text(strip=True)
                if len(spans) > 2:
                    author = spans[2].get_text(strip=True)
                if len(spans) > 3:
                    pages = spans[3].get_text(strip=True)
            rating_span = info_block.select_one(".tjmr-txt")
            if rating_span:
                rating = rating_span.get_text(strip=True)
        reports.append(
            Report(
                title=title,
                link=link,
                published_at=published_at,
                agency=agency,
                author=author,
                rating=rating,
                pages=pages,
            )
        )
        if len(reports) >= limit:
            break
    return reports


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape HuiYunYan research reports.")
    parser.add_argument("--keyword", required=True, help="Search keyword, e.g. 博俊科技")
    parser.add_argument("--limit", type=int, default=20, help="Maximum number of reports")
    args = parser.parse_args()

    try:
        reports = fetch_reports(args.keyword, limit=args.limit)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to fetch reports: {exc}", file=sys.stderr)
        sys.exit(1)

    if not reports:
        print("No reports found.")
        return

    for idx, report in enumerate(reports, start=1):
        print(f"{idx}. {report.title}")
        print(f"   时间: {report.published_at} | 机构: {report.agency} | 作者: {report.author} | 评级: {report.rating} | 页数: {report.pages}")
        print(f"   链接: {report.link}")


if __name__ == "__main__":
    main()
