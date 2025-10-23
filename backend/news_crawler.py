import requests
from bs4 import BeautifulSoup
import re


def fetch_eastmoney_news(url):
    """
    爬取东方财富网新闻内容的函数
    
    Args:
        url (str): 新闻页面的URL
        
    Returns:
        dict: 包含标题、发布时间、正文内容等信息的字典
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            return {'error': f'请求失败，状态码: {response.status_code}'}
        soup = BeautifulSoup(response.text, 'html.parser')
        title_element = soup.find('h1')
        title = title_element.text.strip() if title_element else '未找到标题'
        full_text = soup.get_text()
        daily_selection_content = extract_content_by_keywords(full_text)
        return {
            'title': title,
            'content': daily_selection_content,
            'url': url
        }
    except requests.exceptions.RequestException as e:
        return {'error': f'网络请求错误: {str(e)}'}
    except Exception as e:
        return {'error': f'解析错误: {str(e)}'}


def extract_content_by_keywords(full_text):
    start_keywords = ['每日精选', '每日精選']
    end_keywords = ['财经日历', '交易日历', '财经日曆', '交易日曆']
    start_index = -1
    end_index = -1
    for keyword in start_keywords:
        start_index = full_text.find(keyword)
        if start_index != -1:
            break
    if start_index != -1:
        for keyword in end_keywords:
            end_index = full_text.find(keyword, start_index)
            if end_index != -1:
                break
    if start_index != -1:
        if end_index != -1:
            content = full_text[start_index:end_index].strip()
        else:
            content = full_text[start_index:start_index + 5000].strip()
        content = re.sub(r'\n\s*\n', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        return content
    else:
        return "未找到'每日精选'相关内容"


def main():
    url = "https://finance.eastmoney.com/a/202510223540307843.html"
    print("开始爬取东方财富网新闻...")
    print(f"目标URL: {url}")
    print("-" * 50)
    result = fetch_eastmoney_news(url)
    if 'error' in result:
        print(f"错误: {result['error']}")
    else:
        print(f"标题: {result['title']}")
        print("每日精选内容:")
        print("=" * 50)
        print(result['content'])
        print("=" * 50)
        print(f"内容长度: {len(result['content'])} 字符")


if __name__ == "__main__":
    main()