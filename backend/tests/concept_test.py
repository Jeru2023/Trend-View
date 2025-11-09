# import akshare as ak

# stock_board_concept_index_ths_df = ak.stock_board_concept_index_ths(symbol="磷化工", start_date="20251001", end_date="20251107")
# print(stock_board_concept_index_ths_df)

# import akshare as ak

# stock_board_concept_cons_em_df = ak.stock_board_concept_cons_em(symbol="磷化工")
# print(stock_board_concept_cons_em_df)





# import akshare as ak

# stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
# print(stock_fund_flow_individual_df)

import akshare as ak

concept_name = "磷化工"  # 先用概念字典/解析逻辑确定标准名称
df = ak.stock_board_concept_cons_ths(symbol=concept_name)
print(df.head())