# -*- coding: utf-8 -*-

from __future__ import division
from datetime import timedelta
from pandas import Series, DataFrame
from dateutil.parser import parse
import pandas as pd
def sales_data(start, end):

    delta = (start - end).days

    activities = DataFrame()
    for i in range(delta):
        date = (start + timedelta(days=i+1)).isoformat()
        ipath = '../DE/de-' + date[2:4] + '-' + date[6:7] + '-' + date[8:10] + '.csv'
        df = pd.read_csv(ipath, encoding='utf-8')
        activities = pd.concat([activities, df])

    def get_sales(string):
        sales = float(string[1:])
        return sales
    activities[u'已订购商品销售额'] = activities[u'已订购商品销售额'].apply(get_sales)

    format_ = lambda x: '%.3f' %x
    clicks1 = activities[u'买家访问次数'].sum()
    orders1 = activities[u'已订购商品数量'].sum()
    conversion1 = orders1/clicks1
    sales1 = activities[u'已订购商品销售额'].sum()
    index = [u'买家访问次数', u'已订购商品数量', u'已订购商品销售额', u'订购商品数量转化率']
    output_sr = Series([clicks1, orders1, sales1, conversion1], index=index).apply(format_)

    print output_sr

    asin = raw_input("Input product ASIN： ")
    grouped = activities.groupby(u'（子）ASIN')
    clicks = grouped[u'买家访问次数'].sum()
    clicks.name = unicode('买家访问次数', 'utf-8')
    orders = grouped[u'已订购商品数量'].sum()
    orders.name = unicode('已订购商品数量', 'utf-8')
    conversion = (orders/clicks).apply(format_)
    conversion.name = unicode('订购商品数量转化率', 'utf-8')
    sales = grouped[u'已订购商品销售额'].sum()
    sales.name = unicode('已订购商品销售额', 'utf-8')

    output_df = pd.concat([clicks, orders, sales, conversion], axis=1)
    print output_df.ix[asin]
