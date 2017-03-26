# -*-　coding: utf-8 -*-

from __future__ import division
from dateutil.parser import parse
from datetime import timedelta
from pandas import Series, DataFrame
import pandas as pd
import os
import re


def time_process(start, end):
    '''
    输入时间，返回开始和结束时间
    :return: (start_date, end_date)
    '''
    # 将字符串转换为时间戳
    start_date = parse(start).date()
    end_date = parse(end).date()
    # 输入时间值判断
    if ((start_date > end_date) | (start_date < parse('20170301').date()) | (end_date > parse('20170319').date())):
        start_date = 0
        end_date = 0

    return start_date, end_date


def file_load(country, datatype, start, end):
    '''
    sales_dict和ads_dict 表示国家对应的广告数据和销售数据的文件目录

    datatype= True，打开广告数据. False, 打开销售数据

    start, end传入时间，可为None。目前暂时用于读取销售数据用。

    读销售数据原理：用os.listdir找到数据月份文件夹（如：2017.03），
    根据时间段与文件匹配，读取该时间段内的数据。

    函数返回DataFrame对象

    '''

    print "Data information:"

    ads_dict = {'DE': '../data1/SX/EU/Ads/DE/ads report/', 'ES': '../data1/SX/EU/Ads/ES/ads report/',
                'FR': '../data1/SX/EU/Ads/FR/ads report/', 'IT': '../data1/SX/EU/Ads/IT/ads report/',
                'UK': '../data1/SX/EU/Ads/UK/ads report/', 'JP': '../data1/SX/Japan/Ads/',
                'CA': '../data1/SX/North America/Ads/CA/ads report/',
                'USA': '../data1/SX/North America/Ads/USA/ads report/'}

    sales_dict = {'DE': '../data1/SX/EU/business report/DE/', 'ES': '../data1/SX/EU/business report/ES/',
                  'FR': '../data1/SX/EU/business report/FR/',
                  'IT': '../data1/SX/EU/business report/IT/', 'UK': '../data1/SX/EU/business report/UK/',
                  'JP': '../data1/SX/Japan/business report/',
                  'CA': '../data1/SX/North America/business report/CA/',
                  'USA': '../data1/SX/North America/business report/USA/'}

    if datatype:
        ad_campaign = DataFrame()
        path = ads_dict[country]
        files = os.listdir(path)
        for files_ in files:
            if os.path.isdir(path + files_):  # 找到月份文件夹
                inner_path = path + files_
                files_name = os.listdir(inner_path)
                for file_ in files_name:  # 遍历月份未见家
                    search = re.search("^ADs_SX" + country, file_)
                    if search:
                        df = pd.read_table(inner_path + '/' + file_, sep='\t')
                        ad_campaign = pd.concat([ad_campaign, df])
                        ad_campaign = ad_campaign.drop_duplicates()
                        print ad_campaign.shape
                        print file_
        return ad_campaign
    else:
        sales_df = DataFrame()
        path = sales_dict[country]
        files = os.listdir(path)
        for files_ in files:
            if os.path.isdir(path + files_):  # 找到月份文件夹
                inner_path = path + files_
                delta = (end - start).days
                for i in range(delta + 1):
                    date = (start + timedelta(days=i)).isoformat()
                    ipath = inner_path + '/' + 'SX' + country + '-' + date[2:4] + '-' + date[6:7] + '-' + date[
                                                                                                          8:10] + '.csv'
                    df = pd.read_csv(ipath, encoding='utf-8')
                    sales_df = pd.concat([sales_df, df])
                    sales_df = sales_df.drop_duplicates()
                    print sales_df.shape
                    print date
        return sales_df


def file_process(df, country):
    # 重新索引列名，并且将Start Date 和 End Date 解析为时间
    df.columns = ['Campaign Name', 'Ad Group Name', 'Advertised SKU', 'Keyword', 'Match Type',
                  'Start Date', 'End Date', 'Clicks', 'Impressions', 'CTR',
                  'Total Spend', 'Average CPC', 'Currency', '1-day Orders Placed (#)',
                  '1-day Ordered Product Sales',
                  '1-day Conversion Rate', '1-day Same SKU Units Ordered', '1-day Other SKU Units Ordered',
                  '1-day Same SKU Units Ordered Product Sales', '1-day Other SKU Units Ordered Product Sales',
                  '1-week Orders Placed (#)', '1-week Ordered Product Sales', '1-week Conversion Rate',
                  '1-week Same SKU Units Ordered', '1-week Other SKU Units Ordered',
                  '1-week Same SKU Units Ordered Product Sales',
                  '1-week Other SKU Units Ordered Product Sales', '1-month Orders Placed (#)',
                  '1-month Ordered Product Sales', '1-month Conversion Rate',
                  '1-month Same SKU Units Ordered', '1-month Other SKU Units Ordered',
                  '1-month Same SKU Units Ordered Product Sales',
                  '1-month Other SKU Units Ordered Product Sales']

    if country == 'JP':
        df['Start Date'] = pd.to_datetime(df['Start Date'], yearfirst=True)
        df['End Date'] = pd.to_datetime(df['End Date'], yearfirst=True)
    elif ((country == 'CA') | (country == 'USA')):
        df['Start Date'] = pd.to_datetime(df['Start Date'])
        df['End Date'] = pd.to_datetime(df['End Date'])
    else:
        df['Start Date'] = pd.to_datetime(df['Start Date'], dayfirst=True)
        df['End Date'] = pd.to_datetime(df['End Date'], dayfirst=True)

    # 将total spend和1-day Ordered Product Sales (£) 里的‘，’替换，并转换为浮点型数据
    df['Total Spend'] = df['Total Spend'].apply(str)
    df['1-day Ordered Product Sales'] = df['1-day Ordered Product Sales'].apply(str)

    def rep(string):
        return string.replace(',', '.')

    df['Total Spend'] = df['Total Spend'].apply(rep).apply(float)
    df['1-day Ordered Product Sales'] = df['1-day Ordered Product Sales'].apply(rep).apply(
        float)

    return df


def data_process(grouped):
    '''
    分组后的数据进行计算
    :param grouped:
    :return: output_df
    '''

    orders = grouped['1-day Orders Placed (#)'].sum()  # 第三个维度
    clicks = grouped['Clicks'].sum()  # 第四个维度

    spend = grouped['Total Spend'].sum()  # 第五个维度
    sales = grouped['1-day Ordered Product Sales'].sum()  # 第六个维度

    format_ = lambda x: '%.2f' % x
    # 第一个维度
    conversion = (orders / clicks).apply(format_).replace(['inf', 'nan'], 0.00).apply(float)
    conversion.name = 'Conversion Rate'
    # 第二个维度
    acos = (spend / sales).apply(format_).replace(['inf', 'nan'], 0.00).apply(float)
    acos.name = 'ACOS'

    # 合并数据
    output_df = pd.concat([conversion, acos, orders, clicks, spend, sales], axis=1)
    return output_df


def data_sum(grouped):
    '''
    计算列和
    :param grouped:
    :return: sum_series
    '''
    sum_clicks = grouped['Clicks'].sum()
    sum_orders = grouped['1-day Orders Placed (#)'].sum()
    sum_spend = grouped['Total Spend'].sum()
    sum_sales = grouped['1-day Ordered Product Sales'].sum()
    sum_conversion = sum_orders/sum_clicks
    if sum_sales==0:
        sum_acos = 0
    else:
        sum_acos = sum_spend/sum_sales
    sum_series = Series([sum_clicks, sum_orders, sum_spend, sum_sales, sum_conversion, sum_acos])
    sum_series.index = ['Clicks', '1-day Orders Placed (#)', 'Total Spend',
                            '1-day Ordered Product Sales', 'Average conversion rate', 'Average ACOS']
    return sum_series


def manual_conversion(ad_campaign, sku):
    '''
    计算手动广告转换率
    :param ad_campaign, sku:
    :return: manual_ad_conversion_rate
    '''
    temp_df2 = ad_campaign.set_index(['Advertised SKU', 'Keyword'])
    temp_clicks = temp_df2['Clicks']
    temp_orders = temp_df2['1-day Orders Placed (#)']

    manual_ad_clicks = temp_clicks[sku].sum() - temp_clicks[sku, '*'].sum()

    if (manual_ad_clicks == 0):
        return 0

    manual_ad_orders = temp_orders[sku].sum() - temp_orders[sku, '*'].sum()
    manual_ad_conversion_rate = manual_ad_orders / manual_ad_clicks
    return manual_ad_conversion_rate


def sales_data(start, end, country):
    activities = file_load(country, datatype=False, start=start, end=end)

    activities.columns = ['(Father) ASIN', '(Son) ASIN', '商品名称', 'Total clicks', '买家访问次数百分比',
                          '买家浏览次数', '页面浏览次数百分比', '购买按钮页面浏览率', 'Total ordered', '订单商品数量转化率',
                          'Total ordered product sales', '订单商品种类']

    def get_sales(string):
        string = string.replace(',', '').replace('US', '')
        sales = float(string[1:])
        return sales

    activities['Total ordered product sales'] = activities['Total ordered product sales'].apply(get_sales)

    format_ = lambda x: '%.2f' % x
    clicks1 = activities['Total clicks'].sum()
    orders1 = activities['Total ordered'].sum()
    conversion1 = orders1 / clicks1
    sales1 = activities['Total ordered product sales'].sum()
    unit_price = sales1 / orders1
    index = ['Total clicks', 'Total ordered', 'Total ordered product sales', 'Total conversion rate', 'Unit price']
    output_sr = Series([clicks1, orders1, sales1, conversion1, unit_price], index=index).apply(format_)

    print output_sr

    i=1

    for items in activities['(Son) ASIN']:
        grouped = activities.groupby('(Son) ASIN')
        clicks = grouped['Total clicks'].sum()
        clicks.name = 'Total clicks'
        orders = grouped['Total ordered'].sum()
        orders.name = 'Total ordered'
        conversion = (orders / clicks).apply(format_)
        conversion.name = 'Total conversion rate'
        sales = grouped['Total ordered product sales'].sum()
        sales.name = 'Total ordered product sales'

        output_df = pd.concat([clicks, orders, sales, conversion], axis=1)

        if i==1:
            break

    return output_sr


def time_search(ad_campaign, start, end):
    '''
    输入时间段，查询广告活动业绩信息
    :param ad_campaign:
    :return:
    '''

    # 索引
    row_index = (ad_campaign['Start Date'] >= start) & (ad_campaign['Start Date'] <= end)
    temp_df = ad_campaign.ix[row_index]
    # 重组
    grouped = temp_df.groupby('Campaign Name')
    # 计算
    output_dataframe = data_process(grouped)
    sum_series = data_sum(temp_df)
    # 输出
    print output_dataframe.sort_values('Conversion Rate', ascending=False)

    '''
    预留输出时间段内统计数据表格

    '''

    return sum_series


def sku_search(ad_campaign, start, end):
    '''
    输入时间段和SKU码，查询广告活动业绩信息
    :param ad_campaign:

    :return: None
    '''
    # 索引
    row_index = ((ad_campaign['Start Date'] > start) & (ad_campaign['Start Date'] < end))
    temp_df = ad_campaign.ix[row_index]
    # 重组
    grouped = temp_df.groupby(temp_df['Keyword'])
    temp_df2 = temp_df.set_index(temp_df['Advertised SKU'])
    # 计算
    output_df = data_process(grouped)
    sum_series = data_sum(temp_df2)

    i = 0
    for sku in ad_campaign['Advertised SKU']:
        i = i+1
        temp_index = (ad_campaign['Advertised SKU'] == sku)
        if (ad_campaign.ix[temp_index, 'Keyword'] == '*').any():
            auto_index = ((temp_df['Keyword'] == '*'))
            auto_clicks = temp_df.ix[auto_index, 'Clicks'].sum()
            auto_orders = temp_df.ix[auto_index, '1-day Orders Placed (#)'].sum()
            auto_conversion_rate = auto_orders / auto_clicks
            # 计算手动广告转化率，输出
            manual_ad_conversion_rate = manual_conversion(ad_campaign, sku)
            output_df['Auto ads conversion rate'] = auto_conversion_rate
            output_df['Manual ads conversion rate'] = manual_ad_conversion_rate

            print output_df
        else:
            auto_conversion_rate = 0
            manual_ad_conversion_rate = manual_conversion(ad_campaign, sku)
            output_df['Auto ads conversion rate'] = auto_conversion_rate
            output_df['Manual ads conversion rate'] = manual_ad_conversion_rate

            print output_df

        '''
        预留输出单个SKU统计表格

        '''

        if i == 1:
            break

# 是否还需要根据sku来计算总和？
    return None


def run_main():
    '''
    主函数
    :return:
    '''
    print "Searchable store code: SX"
    print "Searchable country code:  DE, ES, FR, IT, UK, JP, CA, USA"
    print "Searchable date period: 2017/03/01 - 2017/03/19"
    country = raw_input("Input country code: ")
    start = raw_input("Input start date: ")
    end = raw_input("Input end date: ")
    country = country.upper()

    start, end = time_process(start, end)
    if (start, end) == (0, 0):
        print "Wrong date format."
        return None

    print ' '
    print 'Loading...'
    print ' '

    # 载入文件
    ad_campaign = file_load(country, datatype=True, start=None, end=None)

    # 文件处理
    ad_campaign = file_process(ad_campaign, country)

    # 在时间断内，统计转化率等信息
    ads_sr = time_search(ad_campaign, start, end)

    # 对不同sku的产品，统计转化率等信息
    sku_search(ad_campaign, start, end)

    # 对销售数据， 统计转化率等信息
    sales_sr = sales_data(start, end, country)

    total_acos = float(ads_sr['Total Spend'])/float(sales_sr['Total ordered product sales'])
    total_acos = Series(total_acos, index=['Total ACOS'])
    # 合并数据
    total_analysis = pd.concat([ads_sr, sales_sr, total_acos])
    print "Totality analysis: "
    print total_analysis

    '''
    预留输出总体的统计数据表格

    '''


if __name__ == '__main__':
    run_main()
