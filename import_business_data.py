
import arrow
import os 
import pandas as pd
import numpy as np
from clickhouse_driver import Client

from service.order_headers import orders_headers 

client = Client(
  '47.108.76.183',
  port=9000,
  user='default',
  password='ZL9eFE5M',
  database='lai_ge_bao',
  compression=False)
# excel_df = pd.read_excel('./data/A20210318114110.xlsx', header=5)
excel_df = pd.read_excel('./data/A20210318114110.xlsx', skipfooter=1)
excel_df = excel_df.replace(np.nan, '', regex=True)
excel_df['会员卡号'] = excel_df['会员卡号'].apply(str)
excel_df['开台时间'] = pd.to_datetime(excel_df['开台时间'])
excel_df['结算时间'] = pd.to_datetime(excel_df['结算时间'])
headers = orders_headers() 
# print(headers)

sql = 'INSERT INTO orders ({}) VALUES'.format(','.join(headers.keys()))
print(sql)
rows = []
for index, row in excel_df.iterrows():
  d = {}
  for key in headers:
    # print(key)
    d[key] = row[headers[key]]
  
  rows.append(d)
  # print(d)
  # result = client.execute(sql, [d])
print(len(rows))
result = client.execute(sql, rows)
print(result)
