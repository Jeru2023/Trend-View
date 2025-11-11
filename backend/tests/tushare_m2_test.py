import tushare as ts, pandas as pd
token = "2a193feab2591d72d17e53e96773d385d4ed8222399b7564d122a876"
pro = ts.pro_api(token)
start_m = "202211"
end_m   = "202511"
df = pro.cn_m(start_m=start_m, end_m=end_m)
print(len(df))
print(df.head())