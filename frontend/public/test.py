import json
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

FED_NEWS_URL = "https://www.federalreserve.gov/newsevents.htm"
BASE_URL = "https://www.federalreserve.gov"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def extract_fed_press_releases(html_content: str) -> List[dict]:
    """
    从美联储HTML页面提取新闻稿数据（标题、日期、链接）
    """
    soup = BeautifulSoup(html_content, "html.parser")
    press_releases: List[dict] = []

    sections = soup.find_all("div", class_="nePanelBox")
    for section in sections:
        heading = section.find("h4")
        if not heading or "Press Releases" not in heading.get_text():
            continue

        news_items = section.find_all("div", class_="news__item")
        for idx, item in enumerate(news_items[:5]):
            try:
                title_elem = item.find("p", class_="news news__title")
                link = ""
                title = ""
                if title_elem:
                    anchor = title_elem.find("a")
                    if anchor:
                        title = anchor.get_text(strip=True)
                        href = anchor.get("href", "")
                        link = href if href.startswith("http") else f"{BASE_URL}{href}"

                date_elem = item.find("p", class_="time--sm")
                release_date = date_elem.get_text(strip=True) if date_elem else "N/A"

                press_releases.append(
                    {
                        "序号": idx + 1,
                        "标题": title or "N/A",
                        "日期": release_date,
                        "链接": link or "N/A",
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive
                print(f"解析第{idx + 1}条新闻时出错: {exc}")
                continue

    return press_releases


def fetch_html(url: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as exc:
        print(f"请求 {url} 失败: {exc}")
        return None


def get_fed_news_from_url(url: Optional[str] = None, html_content: Optional[str] = None) -> List[dict]:
    """
    从URL或HTML内容获取美联储新闻稿
    """
    if html_content:
        content = html_content
    else:
        target_url = url or FED_NEWS_URL
        content = fetch_html(target_url)
        if content is None:
            return []

    return extract_fed_press_releases(content)


def _is_share_menu(node: BeautifulSoup) -> bool:
    """
    检查节点是否处于分享菜单区域中，分享菜单中的文字无需重复抓取。
    """
    if not node:
        return False
    parent = node.parent
    while parent is not None:
        if parent.has_attr("class") and any(
            "share" in cls or "dropdown-menu" in cls for cls in parent.get("class", [])
        ):
            return True
        parent = parent.parent
    return False


def fetch_press_release_detail(url: str) -> dict:
    """
    抓取新闻稿详情页内容：返回正文和原文文本。
    """
    if not url or url == "N/A":
        return {"内容": "", "原文": ""}

    detail_html = fetch_html(url)
    if detail_html is None:
        return {"内容": "", "原文": ""}

    soup = BeautifulSoup(detail_html, "html.parser")

    article = soup.find(id="article") or soup.find("main") or soup

    texts: List[str] = []
    for node in article.find_all(["p", "li"]):
        if _is_share_menu(node):
            continue
        for anchor in node.select("a.shareLink"):
            anchor.extract()
        text = node.get_text(" ", strip=True)
        if not text:
            continue
        if text not in texts:
            texts.append(text)

    content = "\n".join(texts)
    raw_text = article.get_text("\n", strip=True)

    return {
        "内容": content,
        "原文": raw_text,
    }


def collect_press_releases_with_content(limit: int = 5) -> List[dict]:
    """
    获取指定数量的最新新闻稿并抓取正文内容。
    """
    press_releases = get_fed_news_from_url()
    if not press_releases:
        return []

    enriched: List[dict] = []
    for entry in press_releases[:limit]:
        detail = fetch_press_release_detail(entry.get("链接", ""))
        enriched.append({**entry, **detail})

    return enriched


def display_press_releases(press_releases: List[dict]) -> None:
    """
    格式化显示新闻稿
    """
    if not press_releases:
        print("未找到新闻稿数据")
        return

    print("美联储最新新闻稿 (前5条)")
    print("=" * 80)

    for news in press_releases:
        print(f"\n{news['序号']}. {news['标题']}")
        print(f"   日期: {news['日期']}")
        print(f"   链接: {news['链接']}")
        content = news.get("内容", "")
        if content:
            first_line = content.splitlines()[0] if content.splitlines() else content
            snippet = first_line[:160]
            suffix = "..." if len(first_line) > 160 else ""
            print(f"   内容摘要: {snippet}{suffix}")
        print("-" * 80)


def main() -> None:
    print("\n" + "=" * 80)
    print("方法: 从网站获取实时数据并抓取详情...")
    print("=" * 80)

    press_releases = collect_press_releases_with_content(limit=5)
    if not press_releases:
        print("无法获取实时数据")
        return

    display_press_releases(press_releases)

    df = pd.DataFrame(press_releases)
    df.to_csv("fed_press_releases.csv", index=False, encoding="utf-8-sig")
    with open("fed_press_releases.json", "w", encoding="utf-8") as fp:
        json.dump(press_releases, fp, ensure_ascii=False, indent=2)

    print("\n数据已保存到 fed_press_releases.csv 与 fed_press_releases.json")


if __name__ == "__main__":
    main()
