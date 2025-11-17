"""Client helpers for scraping Sina Finance research reports."""

from __future__ import annotations

import random
import time
from datetime import datetime
from typing import Dict, List

import requests
from bs4 import BeautifulSoup

LIST_URL = "https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml"
DETAIL_BASE = "https://stock.finance.sina.com.cn"


def _build_headers() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Referer": DETAIL_BASE,
    }


def fetch_sina_reports(symbol: str) -> List[dict]:
    """Fetch report list entries for the provided stock symbol."""

    params = {
        "symbol": symbol,
        "t1": "all",
    }
    resp = requests.get(LIST_URL, params=params, headers=_build_headers(), timeout=15)
    resp.raise_for_status()
    resp.encoding = "gbk"
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="tb_01")
    if not table:
        return []

    records: List[dict] = []
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 6:
            continue
        link_tag = cols[1].find("a")
        if not link_tag:
            continue
        detail_href = link_tag.get("href", "").strip()
        if detail_href.startswith("//"):
            detail_href = "https:" + detail_href
        title = link_tag.get_text(strip=True)
        report_type = cols[2].get_text(strip=True)
        publish_date = cols[3].get_text(strip=True)
        org = cols[4].get_text(strip=True)
        analysts = cols[5].get_text(strip=True)
        records.append(
            {
                "title": title,
                "report_type": report_type,
                "publish_date": publish_date,
                "org": org,
                "analysts": analysts,
                "detail_url": detail_href,
            }
        )
    return records


def fetch_sina_report_detail(detail_url: str) -> dict:
    """Fetch detail content from a Sina report detail page."""

    time.sleep(random.uniform(1, 3))
    resp = requests.get(detail_url, headers=_build_headers(), timeout=15)
    resp.raise_for_status()
    resp.encoding = "gbk"
    soup = BeautifulSoup(resp.text, "html.parser")
    title_el = soup.select_one("div.content > h1")
    meta_spans = soup.select("div.content > div.creab span")
    publish_date = None
    org = None
    analysts = None
    for span in meta_spans:
        text = span.get_text(strip=True)
        if text.startswith("日期"):
            publish_date = text.split(":", 1)[-1].strip()
        elif text.startswith("机构"):
            org = span.get_text(" ", strip=True).split(":", 1)[-1].strip()
        elif text.startswith("研究员"):
            analysts = span.get_text(" ", strip=True).split(":", 1)[-1].strip()
    content_block = soup.select_one("div.content div.blk_container")
    content_html = content_block.decode_contents() if content_block else ""
    content_text = content_block.get_text("\n", strip=True) if content_block else ""
    return {
        "title": title_el.get_text(strip=True) if title_el else None,
        "publish_date": publish_date,
        "org": org,
        "analysts": analysts,
        "content_html": content_html,
        "content_text": content_text,
    }


__all__ = ["fetch_sina_reports", "fetch_sina_report_detail"]
