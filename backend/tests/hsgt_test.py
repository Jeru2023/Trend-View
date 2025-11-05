# import akshare as ak

# stock_hsgt_fund_flow_summary_em_df = ak.stock_hsgt_fund_flow_summary_em()
# print(stock_hsgt_fund_flow_summary_em_df)

# import akshare as ak

# stock_hsgt_hist_em_df = ak.stock_hsgt_hist_em(symbol="北向资金")
# print(stock_hsgt_hist_em_df)

# import akshare as ak

# stock_hsgt_board_rank_em_df = ak.stock_hsgt_board_rank_em(symbol="北向资金增持行业板块排行", indicator="今日")
# print(stock_hsgt_board_rank_em_df)

import akshare as ak

stock_board_concept_hist_em_df = ak.stock_board_concept_hist_em(symbol="绿色电力", period="daily", start_date="20220101", end_date="20250227", adjust="")
print(stock_board_concept_hist_em_df)