
import arrow
import os 
import pandas as pd
from sqlalchemy import create_engine,text
from service.query_sql import orders_sql, comments_sql, store_daily_record_sql
from service.convertors import convert_str_2_number
PER_ORDER_AWARD = 5

now = arrow.utcnow()
yesterday = now.shift(months=-1).format('YYYY-MM-DD')
last_month = now.shift(months=-1).format('YYYY-MM-')
#engine = create_engine('mysql+pymysql://datav:!Ed$77zs#q@rm-m5er7r7810lzv6m3rxo.mysql.rds.aliyuncs.com:3306/datav?charset=utf8', echo=False)
# for mac dev env
engine = create_engine('mysql://datav:!Ed$77zs#q@rm-m5er7r7810lzv6m3rxo.mysql.rds.aliyuncs.com:3306/datav', echo=False)
# ordersQuerl = "SELECT * FROM t_store_info_retail_new_5_1 WHERE open_date='{}'".format(yesterday)
orders_sql = orders_sql(last_month)
orders_df = pd.read_sql(text(orders_sql), engine)
# print(orders_df)
comments_sql = comments_sql(last_month)
comments_df = pd.read_sql(text(comments_sql), engine)
comments_df['add_date']= comments_df['add_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
# print(comments_df)

store_records_sql = store_daily_record_sql(last_month)
print(store_records_sql)
store_records_df = pd.read_sql(text(store_records_sql), engine)
#convert string to number
store_records_df['people'] = pd.to_numeric(store_records_df["people"], downcast="float")
print(store_records_df)


def get_daily_comment(store_name, date):
   return comments_df.loc[(comments_df['store_name'] == store_name) & (comments_df['add_date'] == date)] 

def get_store_record(store_name, date):
   return store_records_df.loc[(store_records_df['store_name'] == store_name) & (store_records_df['open_date'] == date)] 

# def convert_str_2_number(str):
#     type(num)

excel_df = pd.read_excel('./daily_motivate.xlsx')
liziba_df = excel_df.loc[excel_df['品牌'] == '李子坝梁山鸡']
# print(liziba_df)
for index, store in liziba_df.iterrows():
    store_df = pd.DataFrame()
    store_orders_df = orders_df.loc[orders_df['store_name'] == store['门店']]
    # print(store_orders_df)
    for i, order in store_orders_df.iterrows():
        # print('store: {},  number of orders: {}'.format(order['store_name'], order['orders']))
        comment = get_daily_comment(order['store_name'], order['open_date'])
        store_record = get_store_record(order['store_name'], order['open_date'])
        # print(store_record['people'])
        data = pd.DataFrame({
            '门店': store['门店'],
            '日期': order['open_date'],
            '安全事故': '无',
            '用户满意度门槛值': store['用户满意度门槛值'],
            '当日在岗人数': store_record['people']
            })
        store_df = store_df.append(data)
    store_df = store_df.reset_index(drop=True)
    store_df.index += 1
    # print(store_df)

    store_df.to_excel('./export/{}-{}.xlsx'.format(store['门店'], last_month), index_label='序号')

        # print(store_record)





# display(HTML(excel_df.to_html()))