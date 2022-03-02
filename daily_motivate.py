
import arrow
import os 
import pandas as pd
from sqlalchemy import create_engine,text
from service.query_sql import * 
from service.convertors import convert_str_2_number
from service.utils import *

PER_ORDER_AWARD = 5
EXPORT_FOLDER = 'exports'
current_dir = current_dir()

#create the export dir if not exists 


now = arrow.utcnow()
# yesterday = now.shift(months=-1).format('YYYY-MM-DD')
# last_month = now.shift(months=-1).format('YYYY-MM')
yesterday = now.shift(months=-1).format('YYYY-MM-DD')
last_month = '2022-02'

end_month = arrow.get(last_month).ceil('month').format('YYYY-MM-DD')
print(end_month)
#engine = create_engine('mysql+pymysql://datav:!Ed$77zs#q@rm-m5er7r7810lzv6m3rxo.mysql.rds.aliyuncs.com:3306/datav?charset=utf8', echo=False)
# for mac dev env
engine = create_engine('mysql://datav:!Ed$77zs#q@rm-m5er7r7810lzv6m3rxo.mysql.rds.aliyuncs.com:3306/datav', echo=False)
# ordersQuerl = "SELECT * FROM t_store_info_retail_new_5_1 WHERE open_date='{}'".format(yesterday)
orders_sql = orders_sql(last_month)
orders_df = pd.read_sql(text(orders_sql), engine)
# print(orders_df)
comments_sql = comments_sql(last_month)
# print(comments_sql)
comments_df = pd.read_sql(text(comments_sql), engine)
comments_df['add_date']= comments_df['add_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
# comments_df['total_rank'] = pd.to_numeric(comments_df["total_rank"], downcast="float")
# print(comments_df)

bad_comments_sql = bad_comments_sql(last_month)
# print(bad_comments_sql)
bad_comments_df = pd.read_sql(text(bad_comments_sql), engine)
bad_comments_df['add_date']= bad_comments_df['add_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
# print(bad_comments_df)

store_records_sql = store_daily_record_sql(last_month)
store_records_df = pd.read_sql(text(store_records_sql), engine)
#convert string to number
store_records_df['people'] = pd.to_numeric(store_records_df["people"], downcast="float")
# print(store_records_df)


def get_daily_comments(store_name, source, date):
    i_date = date 
    while i_date > '{}-01'.format(last_month):
        i_date = arrow.get(i_date).shift(days=-1).format('YYYY-MM-DD')
        comment = comments_df.loc[
            (comments_df['store_name'] == store_name) &
            (comments_df['source'] == source) &
            (comments_df['add_date'] ==i_date)]

        if not comment.empty and comment['total_rank'].iloc[0] != '--':
            return comment
        else:
            print('门店 {} {} 的 {} 记录没有找到'.format(store_name, source, i_date))

    i_date = date
    end_date = arrow.get(last_month).ceil('month').format('YYYY-MM-DD')
    while i_date < end_date:
        i_date = arrow.get(i_date).shift(days=1).format('YYYY-MM-DD')
        comment = comments_df.loc[
            (comments_df['store_name'] == store_name) &
            (comments_df['source'] == source) &
            (comments_df['add_date'] ==i_date)]

        if not comment.empty and comment['total_rank'].iloc[0] != '--':
            return comment
        else:
            print('门店 {} {} 的 {} 记录没有找到'.format(store_name, source, i_date))
    return pd.DataFrame()


def get_bad_comments(store_name, date):
   return bad_comments_df.loc[
       (bad_comments_df['store_name'] == store_name) &
       (bad_comments_df['add_date'] == date)] 

def get_store_record(store_name, date):
   return store_records_df.loc[(store_records_df['store_name'] == store_name) & (store_records_df['open_date'] == date)] 

excel_df = pd.read_excel('./daily_motivate.xlsx')

def generate_excel_files(brand):
    liziba_df = excel_df.loc[excel_df['品牌'] == brand]
    # print(liziba_df)
    for index, store in liziba_df.iterrows():
        store_df = pd.DataFrame()
        store_orders_df = orders_df.loc[orders_df['store_name'] == store['门店']]
        store_orders_df = store_orders_df.reset_index(drop=True)
        if store['门店'] == '受气牛肉三峡广场店':
            other_orders_df = orders_df.loc[orders_df['store_name'] == '李子坝梁山鸡三峡广场店']
            other_orders_df = other_orders_df.reset_index(drop=True)
            store_orders_df['orders'] = store_orders_df['orders'] + other_orders_df['orders']
        
        # print(store_orders_df)
        for i, order in store_orders_df.iterrows():
            # print('store: {}, date: {}, number of orders: {}'.format(order['store_name'], order['open_date'], order['orders']))
            comment = get_daily_comments(store['别名'], '大众点评', order['open_date'])
            take_out_comment = get_daily_comments(store['别名'], '美团外卖', order['open_date'])
            store_record = get_store_record(order['store_name'], order['open_date'])
            # print(comment)
            store_name = store['门店']
            open_date = order['open_date']
            satisfaction_score = store['用户满意度门槛值']
            dianping_score = 0
            meet_comment_goal = '达标'
            comment_score = '--' 
            take_out_score = 0
            staff_on_duty = 0

            if comment.empty:
                print('!! 门店: {}, 日期: {} 没有找到大众点评评价记录!!'.format(store_name, open_date))
            else:
                dianping_score = float(comment['total_rank'].iloc[0])


            if take_out_comment.empty:
                print('!! 门店: {}, 日期: {} 没有找到美团外卖评价记录 !!'.format(store_name, open_date))
            else:
                score = take_out_comment['total_rank'].iloc[0]
                if score != '--':
                    take_out_score = float(score)

            if store_record.empty:
                print('!! 门店: {}, 日期: {} 没有找到门店该天记录 !!'.format(store_name, open_date))
            else:
                staff_on_duty = store_record['people'].iloc[0]

            #计算订单量激励
            staff_avg = '--'
            if staff_on_duty:
                # staff_avg = round(order['orders'] / staff_on_duty, 1)
                staff_avg = order['orders'] // staff_on_duty

            #计算满意度激励
            satisfaction_motivate = 0
            if dianping_score != 0 and take_out_score != 0:
                comment_score = round((dianping_score + take_out_score) / 2, 2)
                if satisfaction_score > comment_score:
                    meet_comment_goal = '未达标'
            elif dianping_score != 0 or take_out_score != 0:
                comment_score  = dianping_score + take_out_score
                if satisfaction_score > comment_score:
                    meet_comment_goal = '未达标'


            if meet_comment_goal == '达标' and comment_score != '--' and comment_score >= store['评价激励达标']:
                satisfaction_motivate = 5 

            #差评加减
            no_bad_comment = 5
            bad_comment = get_bad_comments(store['别名'], order['open_date'])
            if not bad_comment.empty:
                print('!! 门店: {}, 日期: {} 有差评 !!'.format(store_name, open_date))
                no_bad_comment = -5 
            

            order_motivate = 0
            baseline_orders = int(store['基础单量'])
            # print(type(staff_avg))
            order_meet_goal = '未达标'
            if not isinstance(staff_avg, str) and staff_avg >= baseline_orders:
                order_meet_goal = '达标'
                if meet_comment_goal == '达标':
                    order_motivate += 5
                    order_motivate += (staff_avg - baseline_orders) * 5 
            
            daily_total = order_motivate + satisfaction_motivate + no_bad_comment
            if daily_total < 0:
                daily_total = 0


            data = pd.DataFrame({
                '门店': [store_name],
                '日期': [open_date],
                '安全事故': ['无'],
                '用户满意度门槛值': [satisfaction_score],
                '评价激励达标门槛': [store['评价激励达标']],

                # '评价分数': [dianping_score],
                # '用户满意度达标': [meet_dianping_score],
                '当日在岗人数': [staff_on_duty],
                '基础单量': [store['基础单量']],
                '订单数': [order['orders']],
                '人均单量': [staff_avg],
                '单量达标': [order_meet_goal],
                '订单激励': [order_motivate],
                # '外卖评分': [take_out_score],
                # '外卖满意度达标': [meet_take_out_score],
                # '用户满意度激励': [satisfaction_motivate],
                '平台评价分数': [comment_score],
                '用户满意度达标': [meet_comment_goal],
                '用户满意度激励': [satisfaction_motivate],
                '表扬激励': [0],
                '差评加减': [no_bad_comment],
                '每日激励汇总': [daily_total],
                '每日激励成本': [daily_total * staff_on_duty]
                })
            store_df = store_df.append(data)
        store_df = store_df.reset_index(drop=True)
        store_df.index += 1

        # print(store_df)

        if not store_df.empty:
            store_df.loc['合计']= store_df.sum(numeric_only=True, axis=0)
            brand_folder = '{}-{}'.format(brand, last_month)
            create_dir_if_not_exists('{}/{}/{}'.format(current_dir, EXPORT_FOLDER, brand_folder))
            store_df.to_excel('./{}/{}/{}-{}.xlsx'.format(EXPORT_FOLDER , brand_folder, store['门店'], last_month), index_label='序号')
        else:
            print('!! store {} not data !!'.format(store['门店']))

create_dir_if_not_exists('{}/{}'.format(current_dir, EXPORT_FOLDER))
generate_excel_files('李子坝梁山鸡')
generate_excel_files('受气牛肉')
generate_excel_files('三斤耗儿鱼')

# meituan_df = get_daily_comments('李子坝梁山鸡源著天街店', '美团外卖', '2021-03-05')
# print(meituan_df)
