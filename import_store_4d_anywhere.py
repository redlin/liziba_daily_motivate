import pandas as pd
import numpy as np
import os
import platform
import subprocess
import datetime
import json
import requests
import re
from sqlalchemy import Column, String, ForeignKey, String, Float, DateTime, Integer, create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT
# from flask import current_app as app
Base = declarative_base()

def convert_score(s):
    if isinstance(s, int) or isinstance(s, float):
        return s
    if not s or s == '--':
        return 0
    try:
        return float(s.replace('%', ''))
    except ValueError:
        return float(s)

def store_name_convertor(name):
    if (re.match(r'李子坝梁山鸡\S*老店', name)):
        return '李子坝梁山鸡老店'
    if (re.match(r'李子坝梁山鸡\S*公园', name)):
        return '李子坝梁山鸡公园店'
    if (re.match(r'李子坝梁山鸡\S*渝北', name)):
        return '李子坝梁山鸡渝北店'
    if (re.match(r'李子坝梁山鸡\S*长嘉汇', name)):
        return '李子坝梁山鸡长嘉汇店'
    if (re.match(r'李子坝梁山鸡\S*北碚万达', name)):
        return '李子坝梁山鸡北碚万达店'
    if (re.match(r'李子坝梁山鸡\S*观音桥', name)):
        return '李子坝梁山鸡观音桥店'
    if (re.match(r'李子坝梁山鸡\S*九龙滨江', name)):
        return '李子坝梁山鸡九龙滨江店'
    if (re.match(r'李子坝梁山鸡\S*解放碑英利', name)):
        return '李子坝梁山鸡解放碑英利店'
    if (re.match(r'李子坝梁山鸡\S*磁器口', name)):
        return '李子坝梁山鸡磁器口店'
    if (re.match(r'李子坝梁山鸡\S*南坪', name)):
        return '李子坝梁山鸡南坪店'
    if (re.match(r'李子坝梁山鸡\S*大学城', name)):
        return '李子坝梁山鸡大学城店'
    if (re.match(r'李子坝梁山鸡\S*回兴', name)):
        return '李子坝梁山鸡回兴店'
    if (re.match(r'李子坝梁山鸡\S*源著', name)):
        return '李子坝梁山鸡源著天街店'
    if (re.match(r'李子坝梁山鸡\S*来福士', name)):
        return '李子坝梁山鸡来福士店'
    if (re.match(r'李子坝梁山鸡\S*三峡广场', name)):
        return '李子坝梁山鸡三峡广场店'
    if (re.match(r'三斤耗儿鱼\S*北仓', name)):
        return '三斤耗儿鱼北仓店'
    if (re.match(r'三斤耗儿鱼\S*东原1891', name)):
        return '三斤耗儿鱼东原1891店'
    if (re.match(r'三斤耗儿鱼\S*九龙滨', name)):
        return '三斤耗儿鱼九龙滨江店'
    if (re.match(r'受气牛肉\S*九坑子', name)):
        return '受气牛肉九坑子老社区店'
    if (re.match(r'受气牛肉\S*观音桥', name)):
        return '受气牛肉观音桥店'
    if (re.match(r'受气牛肉\S*弹子石', name)):
        return '受气牛肉弹子石老街店'
    if (re.match(r'受气牛肉\S*磁器口', name)):
        return '受气牛肉磁器口店'
    if (re.match(r'受气牛肉\S*水晶郦城', name)):
        return '受气牛肉水晶郦城店'
    if (re.match(r'受气牛肉\S*凤天路', name)):
        return '受气牛肉凤天路店'
    if (re.match(r'受气牛肉\S*南坪', name)):
        return '受气牛肉南坪店'
    if (re.match(r'受气牛肉\S*望京', name)):
        return '受气牛肉望京店'
    if (re.match(r'受气牛肉\S*大坪', name)):
        return '受气牛肉大坪老社区店'
    if (re.match(r'受气牛肉\S*三峡', name)):
        return '受气牛肉三峡广场店'
    if (re.match(r'受气牛肉\S*解放碑', name)):
        return '受气牛肉解放碑店'
    if (re.match(r'受气牛肉\S*大石坝', name)):
        return '受气牛肉大石坝店'
    if (re.match(r'受气牛肉\S*空港', name)):
        return '受气牛肉空港店'
    if (re.match(r'受气牛肉\S*爱琴海', name)):
        return '受气牛肉爱琴海店'
    if (re.match(r'受气牛肉\S*大学城', name)):
        return '受气牛肉大学城店'
    if (re.match(r'受气牛肉\S*回兴', name)):
        return '受气牛肉回兴店'
    if (re.match(r'受气牛肉\S*来福士店', name)):
        return '受气牛肉来福士店'
    if (re.match(r'火烧溪黄辣丁\S*大坪', name)):
        return '火烧溪黄辣丁大坪店'
    if (re.match(r'沸堂蛙\S*大坪', name)):
        return '沸堂蛙大坪店'
    if (re.match(r'受气牛肉\S*大兴店', name)):
        return '受气牛肉大兴店'
    if (re.match(r'李子坝梁山鸡\S*东原悦荟', name)):
        return '李子坝梁山鸡东原悦荟店'
    if (re.match(r'李子坝梁山鸡\S*西城天街', name)):
        return '李子坝梁山鸡西城天街店'
    if (re.match(r'受气牛肉\S*万象城', name)):
        return '受气牛肉万象城店'
    if (re.match(r'受气牛肉\S*融创文旅', name)):
        return '受气牛肉融创文旅城店'
    if (re.match(r'受气牛肉\S*融创茂', name)):
        return '受气牛肉融创文旅城店'
    if (re.match(r'李子坝梁山鸡\S*融创茂', name)):
        return '李子坝梁山鸡融创茂店'
    print('no match name {}'.format(name))
    
    return name

class Store4D(Base):
    # __tablename__ = "t_store_4d_evaluation_dev"
    __tablename__ = "t_store_4d_evaluation"
    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer)
    yearmonth = Column(String)
    brand_name = Column(String)
    store_name = Column(String)
    anytime_anywhere_score = Column(Float)
    return_visit_score = Column(Float)
    # cleanup_inspect_score = Column(Float)
    cleanup_inspect_overall = Column(Float)
    dianping_average = Column(Float)
    meituan_average = Column(Float)
    ele_average = Column(Float)
    mystery_customer_1 = Column(Float)
    mystery_customer_2 = Column(Float)
    mystery_customer_average = Column(Float)
    repurchase_rate = Column(Float)
    qc_store_manager = Column(Float)
    qc_chef = Column(Float)
    qc_total = Column(Float)
    qc_score = Column(Float)
    qc_rank = Column(Float)

anywhere_folder = '/Users/redlin/Downloads/store4d/anytime_anywhere/'
mystery_customer = '/Users/redlin/Downloads/store4d/mystery_customer'
quality_control = '/Users/redlin/Downloads/store4d/quality_control'


# engine = create_engine('mysql+pymysql://datav:Liziba123@rm-m5er7r7810lzv6m3rxo.mysql.rds.aliyuncs.com:3306/datav?charset=utf8', echo=False)
engine = create_engine('mysql://datav:!Ed$77zs#q@rm-m5er7r7810lzv6m3rxo.mysql.rds.aliyuncs.com:3306/datav?charset=utf8', echo=False)
DBSession = sessionmaker(bind=engine)
session = DBSession()

def get_files(dir) :
    files = []
    for dirpath, _, filenames in os.walk(dir):
        for f in filenames:
            if ('.xlsx' in f  or '.xls'in f) and not f.startswith('~'):
                path = os.path.join(dirpath, f)
                print(path)
                files.append(path)
    return files

def import_anywhere_data():
    print('importing anytime anywhere data...')
    excel_files = get_files(anywhere_folder)
    if excel_files :
        frames = []
        for file in excel_files:
            # df = pd.read_excel(file, converters={'分数': convert_score})
            df = pd.read_excel(file, converters={'门店': store_name_convertor})
            # df = raw_df.replace(np.nan, '', regex=True)
            frames.append(df)
        rows = pd.concat(frames)
        print(rows)
        # try:
        for index, row in rows.iterrows():
            item =  session.query(Store4D).filter(and_(
                Store4D.yearmonth==row['月份'], 
                Store4D.store_name==row['门店']
            )).first()
            if not item:
                print('随时随地 月份:{} 门店:{} 未导入数据库，开始导入'.format(row['月份'], row['门店']))
                new_item = Store4D(
                    yearmonth=row['月份'],
                    brand_name=row['品牌'],
                    store_name=row['门店'],
                    anytime_anywhere_score=row['分数']
                )
                session.add(new_item)
            else:
                print('随时随地 月份:{} 门店:{} 已经导入数据库, 现在更新'.format(row['月份'], row['门店']))
                item.anytime_anywhere_score=row['分数']


        session.commit()
                # session.close()
                
        # except Exception as e:
        #     print('to_sql fail', e)
        #     return
    else:
        print('no anywhere data')

def import_qc_data():
    print('importing quality data...')
    excel_files = get_files(quality_control)
    if excel_files :
        frames = []
        for file in excel_files:
            df = pd.read_excel(file, converters={'店长': convert_score, '合计': convert_score, '厨师长': convert_score, '门店': store_name_convertor})
           # df = raw_df.replace(np.nan, '', regex=True)
            frames.append(df)
        rows = pd.concat(frames)
        print(rows)
        # try:
        for index, row in rows.iterrows():
            item =  session.query(Store4D).filter(and_(
                Store4D.yearmonth==row['月份'], 
                Store4D.store_name==row['门店']
            )).first()
            if not item:
                print('品控 月份:{} 门店:{} 未导入数据库，开始导入'.format(row['月份'], row['门店']))
                new_item = Store4D(
                    yearmonth=row['月份'],
                    brand_name=row['品牌'],
                    store_name=row['门店'],
                    qc_store_manager=row['店长'],
                    qc_chef=row['厨师长'],
                    qc_total=row['合计'],
                    qc_score=row['得分']
                )
                session.add(new_item)
            else:
                item.qc_store_manager=row['店长']
                item.qc_chef=row['厨师长']
                item.qc_total=row['合计']
                item.qc_score=row['得分']
                print('品控 月份:{} 门店:{} 已经导入数据库'.format(row['月份'], row['门店']))

        session.commit()
                
        # except Exception as e:
        #     print('to_sql fail', e)
        #     return
    else:
        print('no quality control data')


def import_mystery_customer():
    print('importing mystery_customer...')
    excel_files = get_files(mystery_customer)
    if excel_files :
        frames = []
        for file in excel_files:
            raw_df = pd.read_excel(file, converters={'门店': store_name_convertor})
            df = raw_df.replace(np.nan, '', regex=True)
            frames.append(df)
        rows = pd.concat(frames)
        print(rows)
        # try:
        for index, row in rows.iterrows():
            item =  session.query(Store4D).filter(and_(
                Store4D.yearmonth==row['月份'], 
                Store4D.store_name==row['门店']
            )).first()
            if not item:
                print('神秘顾客 月份:{} 门店:{} 未导入数据库，开始导入'.format(row['月份'], row['门店']))
                new_item = Store4D(
                    yearmonth=row['月份'],
                    brand_name=row['品牌'],
                    store_name=row['门店'],
                    mystery_customer_1=row['第一轮'],
                    mystery_customer_2=row['第二轮'],
                    mystery_customer_average=row['平均分']
                )
                session.add(new_item)
            else:
                item.mystery_customer_1=row['第一轮']
                item.mystery_customer_2=row['第二轮']
                item.mystery_customer_average=row['平均分']
                print('神秘顾客 月份:{} 门店:{} 已经导入数据库'.format(row['月份'], row['门店']))

        session.commit()
                # session.close()
                
        # except Exception as e:
        #     print('to_sql fail', e)
        #     return
    else:
        print('no mystery customer data')
    
if __name__ == '__main__':
    # import_anywhere_data()
    # import_qc_data()
    import_mystery_customer()
    session.close()
