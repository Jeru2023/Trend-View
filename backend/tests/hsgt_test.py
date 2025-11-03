# -*- coding:utf-8 -*-
# !/usr/bin/env python
"""
Date: 2025/3/4 23:00
Desc: 东方财富网-数据中心-沪深港通持股
https://data.eastmoney.com/hsgtcg/
沪深港通详情: https://finance.eastmoney.com/news/1622,20161118685370149.html
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup

from akshare.utils.tqdm import get_tqdm
from akshare.utils.func import fetch_paginated_data


def stock_hsgt_fund_flow_summary_em() -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-沪深港通资金流向
    https://data.eastmoney.com/hsgt/index.html#lssj
    :return: 沪深港通资金流向
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    # url = "https://datacenter-web.eastmoney.com//web/api/data/v1/get?reportName=RPT_MUTUAL_DEALAMT&columns=ALL&filter=(TRADE_DATE%3E=%272023-11-03%27)&sortTypes=-1&sortColumns=TRADE_DATE&source=WEB&client=WEB&callback=jQuery112307412374929427085_1762184509045&_=1762184509050"
    params = {
        "reportName": "RPT_MUTUAL_DEALAMT",
        "columns": "ALL",
        "quoteColumns": "status~07~BOARD_CODE,dayNetAmtIn~07~BOARD_CODE,dayAmtRemain~07~BOARD_CODE,"
        "dayAmtThreshold~07~BOARD_CODE,f104~07~BOARD_CODE,f105~07~BOARD_CODE,"
        "f106~07~BOARD_CODE,f3~03~INDEX_CODE~INDEX_f3,netBuyAmt~07~BOARD_CODE",
        "quoteType": "0",
        "pageNumber": "1",
        "pageSize": "2000",
        "sortTypes": "-1",
        "sortColumns": "TRADE_DATE",
        "source": "WEB",
        "client": "WEB",
    }

    r = requests.get(url, params=params)
    print(r)
    data_json = r.json()
    print(data_json)
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.columns = [
        "交易日",
        "-",
        "类型",
        "板块",
        "资金方向",
        "-",
        "相关指数",
        "-",
        "交易状态",
        "资金净流入",
        "当日资金余额",
        "-",
        "上涨数",
        "下跌数",
        "持平数",
        "指数涨跌幅",
        "成交净买额",
    ]
    temp_df = temp_df[
        [
            "交易日",
            "类型",
            "板块",
            "资金方向",
            "交易状态",
            "成交净买额",
            "资金净流入",
            "当日资金余额",
            "上涨数",
            "持平数",
            "下跌数",
            "相关指数",
            "指数涨跌幅",
        ]
    ]
    temp_df["交易日"] = pd.to_datetime(temp_df["交易日"], errors="coerce").dt.date
    temp_df["成交净买额"] = pd.to_numeric(temp_df["成交净买额"], errors="coerce")
    temp_df["资金净流入"] = pd.to_numeric(temp_df["资金净流入"], errors="coerce")
    temp_df["当日资金余额"] = pd.to_numeric(temp_df["当日资金余额"], errors="coerce")
    temp_df["上涨数"] = pd.to_numeric(temp_df["上涨数"], errors="coerce")
    temp_df["持平数"] = pd.to_numeric(temp_df["持平数"], errors="coerce")
    temp_df["下跌数"] = pd.to_numeric(temp_df["下跌数"], errors="coerce")
    temp_df["指数涨跌幅"] = pd.to_numeric(temp_df["指数涨跌幅"], errors="coerce")
    temp_df["成交净买额"] = temp_df["成交净买额"] / 10000
    temp_df["资金净流入"] = temp_df["资金净流入"] / 10000
    temp_df["当日资金余额"] = temp_df["当日资金余额"] / 10000
    return temp_df

df = stock_hsgt_fund_flow_summary_em()
print(df)