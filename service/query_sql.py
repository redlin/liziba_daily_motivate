

def orders_sql(date):
  return """
    select brand_name, store_name, open_date, count(*) as orders \
    FROM datav.t_bill_info_detail where open_date like '{}-%' and actual_money >= 80\
    group by store_name, open_date;
  """.format(date)

# def comments_sql(date):
#   return """
#     select store_name, add_date, brand_name, avg(total_rank) \
#     FROM datav.t_store_comments \
#     where add_date like \"{}%\" \
#     and (brand_name=\"受气牛肉\" or  brand_name=\"李子坝梁山鸡\" \
#     or brand_name=\"三斤耗儿鱼\") \
#     group by store_name, add_date;
#   """.format(date)

def comments_sql(date):
  return """
    select store_name, add_date, brand_name, source, total_rank \
    FROM datav.t_store_comments \
    where add_date like \"{}-%\" \
    and (brand_name=\"受气牛肉\" or  brand_name=\"李子坝梁山鸡\" \
    or brand_name=\"三斤耗儿鱼\") \
    group by store_name, source, add_date;
  """.format(date)

def bad_comments_sql(date):
  return """
    select c_storeName2 as store_name, c_datetime as add_date, \
     c_sourceName as source, c_storeName1 as brand_name, c_score as score\
    FROM datav.customer_e\
    where c_datetime like \"{}-%\" \
    and c_score < 3.5 \
    and c_handle > 0 and c_handle < 10 \
    and (c_storeName1=\"受气牛肉\" or  c_storeName1=\"李子坝梁山鸡\" \
    or c_storeName1=\"三斤耗儿鱼\") \
    group by store_name, c_sourceName, c_handle;
  """.format(date)

def store_daily_record_sql(date):
  return """
    select store_name, open_date, brand_name, reserve_7 as people \
    FROM datav.t_store_retail_back_write \
    where open_date like \"{}-%\" \
    and (brand_name=\"受气牛肉\" or  brand_name=\"李子坝梁山鸡\" \
    or brand_name=\"三斤耗儿鱼\") \
    group by store_name, open_date;
  """.format(date)