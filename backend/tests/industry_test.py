import akshare as ak

stock_board_industry_hist_em_df = ak.stock_board_industry_hist_em(symbol="小金属", start_date="20211201", end_date="20240222", period="日k", adjust="")
print(stock_board_industry_hist_em_df)