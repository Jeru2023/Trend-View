from backend.src.api_clients.eastmoney_news import fetch_eastmoney_detail
import time

urls = [
    'https://finance.eastmoney.com/a/202510223540307843.html',
    'https://finance.eastmoney.com/a/202510223540307844.html',
]

for url in urls:
    detail = fetch_eastmoney_detail(url)
    print(url, bool(detail.content), len(detail.content or ''))
    time.sleep(1)
