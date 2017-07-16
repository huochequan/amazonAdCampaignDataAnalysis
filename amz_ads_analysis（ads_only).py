# -*-　coding: utf-8 -*-

'''
    作者： 陈广
    时间：2017-7-10
    版本号： 20170710
    程序简介：读入亚马逊各个站点的广告活动数据，
    对其某个时段的订单数，点击量，销售量，销售额，ACOS，
    转化率，CTR等进行统计计算。最后在Result文件夹输出excel文件。

    注：在运行程序前，在第86行修改文件的读取目录，即可正常运行

'''

from __future__ import division
from dateutil.parser import parse
from datetime import timedelta
from pandas import Series, DataFrame
import datetime
import xlsxwriter
import diagnose
import pandas as pd
import os


class AmzAdsAnalysis:
    def __init__(self):
        self.start = None
        self.end = None
        self.country = None
        self.store = None

    def time_process(self):
        '''
        输入时间，返回开始和结束时间
        :return: (start_date, end_date)
        '''
        # 将字符串转换为时间戳
        start_date = parse(self.start).date()
        end_date = parse(self.end).date()


        return start_date, end_date


    def file_load(self):
        '''
        sales_dict和ads_dict 表示国家对应的广告数据和销售数据的文件目录

        datatype= True，打开广告数据. False, 打开销售数据

        start, end传入时间，可为None。目前暂时用于读取销售数据用。

        读销售数据原理：用os.listdir找到数据月份文件夹（如：2017.03），
        根据时间段与文件匹配，读取该时间段内的数据。

        函数返回DataFrame对象

        '''

        ads_dict = {
            'SXDE': '/data/SX/EU/Ads/DE/ads report/',
            'SXES': '/data/SX/EU/Ads/ES/ads report/',
            'SXFR': '/data/SX/EU/Ads/FR/ads report/',
            'SXIT': '/data/SX/EU/Ads/IT/ads report/',
            'SXUK': '/data/SX/EU/Ads/UK/ads report/',
            'SXJP': '/data/SX/Japan/Ads/',
            'SXCA': '/data/SX/North America/Ads/CA/ads report/',
            'SXUS': '/data/SX/North America/Ads/USA/ads report/',
            'HYYDE': '/data/HYY/EU/ads/DE/',
            'HYYES': '/data/HYY/EU/ads/ES/',
            'HYYFR': '/data/HYY/EU/ads/FR/',
            'HYYIT': '/data/HYY/EU/ads/IT/',
            'HYYUK': '/data/HYY/EU/ads/UK/',
            'HYYJP': '/data/HYY/Japan/Ads/',
            'HYYUS': '/data/HYY/North America/ads/USA/ads report/',
            'TXHLDE': '/data/TXHL/EU/ads/DE/',
            'TXHLES': '/data/TXHL/EU/ads/ES/',
            'TXHLFR': '/data/TXHL/EU/ads/FR/',
            'TXHLIT': '/data/TXHL/EU/ads/IT/',
            'TXHLUK': '/data/TXHL/EU/ads/UK/',
            'TXHLJP': '/data/TXHL/Japan/ads/',
            'TXHLCA': '',
            'TXHLUS': '',
        }

        ad_campaign = DataFrame()
        path = 'F:/PycharmFile' + ads_dict[self.store + self.country]   #广告数据文件存放位置
        file_fold = self.end.strftime('%Y') + '.' + self.end.strftime('%m')
        # 需修改： 直接写出文件夹名，文件名file_name，如果存在，则打开文件，不存在，则查找
        file_name = "ADs_" + self.store + self.country + "_" + str(self.end.year) + "-" \
                    + str(self.end.month) + "-" + str(self.end.day) + ".txt"
        if os.path.isdir(path + file_fold):  # 找到月份文件夹

            if os.path.isfile(path + file_fold + "/" + file_name):
                pass
            else:
                print 'There is no such file at ' + path + file_fold + "/" + file_name

            if self.country == "JP":
                ad_campaign = pd.read_table(path + file_fold + "/" + file_name, sep='\t', encoding='Shift-JIS')
            else:
                ad_campaign = pd.read_table(path + file_fold + "/" + file_name, sep='\t', encoding='utf-8')
        else:
            print 'There is no such file at ' + path + file_fold + "/" + file_name

        return ad_campaign

    def file_process(self, df):
        '''
        文件载入后进行处理
        :param df:
        :return:
        '''
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

        if self.country == 'JP':
            df['Start Date'] = pd.to_datetime(df['Start Date'], yearfirst=True)
            df['End Date'] = pd.to_datetime(df['End Date'], yearfirst=True)
        elif ((self.country == 'CA') | (self.country == 'US')):
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


    def data_process(self, grouped):
        '''
        计算转化率，ACOS， 订单数，点击量， 总花费， 总销售额
        :param grouped:
        :return: output_df
        '''

        orders = grouped['1-day Orders Placed (#)'].sum()  # 第三个维度
        clicks = grouped['Clicks'].sum()  # 第四个维度

        spend = grouped['Total Spend'].sum()  # 第五个维度
        sales = grouped['1-day Ordered Product Sales'].sum()  # 第六个维度


        impressions = grouped['Impressions'].sum()

        format_ = lambda x: '%.2f' % x
        # 第一个维度
        conversion = (orders / clicks).apply(format_).replace(['inf', 'nan'], 0.00).apply(float)
        conversion.name = 'Conversion Rate'
        # 第二个维度
        acos = (spend / sales).apply(format_).replace(['inf', 'nan'], 0.00).apply(float)
        acos.name = 'ACOS'

        ctr = (clicks/impressions).replace(['inf', 'nan'], 0).apply(float)
        ctr.name = 'CTR'

        # 合并数据
        output_df = pd.concat([impressions, ctr, clicks, conversion, sales, acos, orders, spend], axis=1)
        return output_df


    def data_sum(self, grouped):
        '''
        计算列和
        :param grouped:
        :return: sum_series
        '''
        format_ = lambda x: '%.2f' % x
        sum_clicks = grouped['Clicks'].sum()
        sum_impressions = grouped['Impressions'].sum()
        sum_orders = grouped['1-day Orders Placed (#)'].sum()
        sum_spend = grouped['Total Spend'].sum()
        sum_sales = grouped['1-day Ordered Product Sales'].sum()
        if sum_clicks == 0:
            sum_conversion = 0
        else:
            sum_conversion = sum_orders/sum_clicks

        if sum_impressions == 0:
            sum_ctr = 0
        else:
            sum_ctr = sum_clicks/sum_impressions

        if sum_sales==0:
            sum_acos = 0
        else:
            sum_acos = sum_spend/sum_sales
        sum_series = Series([sum_clicks, sum_orders, sum_spend, sum_sales, sum_conversion, sum_acos, sum_ctr]).apply(format_)
        sum_series.index = ['Clicks', '1-day Orders Placed (#)', 'Total Spend',
                                '1-day Ordered Product Sales', 'Average conversion rate', 'Average ACOS', 'Average CTR']
        return sum_series


    def manual_conversion(self, df):
        '''
        计算手动广告转换率
        :param ad_campaign, sku:
        :return: manual_ad_conversion_rate
        '''
        print df

        df_clicks = df['Clicks'].sum()
        print df_clicks
        df_orders = df['1-day Orders Placed (#)'].sum()

        manual_ad_clicks = df_clicks - df.ix["*", "Clicks"]
        print manual_ad_clicks
        print type(manual_ad_clicks)

        if not manual_ad_clicks:
            return 0

        manual_ad_orders = df_orders - df.ix["*", "1-day Orders Placed (#)"]
        manual_ad_conversion_rate = manual_ad_orders / manual_ad_clicks
        return manual_ad_conversion_rate



    def time_search(self, ad_campaign, writer):
        '''
        查询各个广告活动业绩信息
        :param ad_campaign:
        :return:
        '''

        print " "
        print "ADs Campaign Data"

        # 索引
        row_index = (ad_campaign['Start Date'] >= self.start) & (ad_campaign['Start Date'] <= self.end)
        temp_df = ad_campaign.ix[row_index]
        # 重组
        grouped = temp_df.groupby('Campaign Name')

        # 计算
        output_dataframe = self.data_process(grouped)

        sum_series = self.data_sum(temp_df)

        output_dataframe.sort_values('Impressions', ascending=False).to_excel(writer, sheet_name='ADS_Campaign')
        days = (self.end - self.start).days
        writer = diagnose.diagnose(writer, 'ADS_Campaign', output_dataframe.shape, days)


        return sum_series


    def sku_search(self,ad_campaign, writer):
        '''
        按SKU码，查询广告活动业绩信息，计算各个关键词的转化率信息
        :param ad_campaign:

        :return: None
        '''
        # 索引
        row_index = ((ad_campaign['Start Date'] >= self.start) & (ad_campaign['Start Date'] <= self.end))
        temp_df = ad_campaign.ix[row_index]
        # 重组
        grouped = temp_df.groupby([temp_df['Advertised SKU'], temp_df['Keyword']])
        # 计算
        output_df = self.data_process(grouped)

        for sku in output_df.index.levels[0]:
            print ""
            print sku
            df_temp = output_df.ix[sku]
            if (df_temp.index == "*").any():
                auto_conversion_rate = output_df.ix[(sku, "*"), "Conversion Rate"]
                # 计算手动广告转化率，输出
                manual_ad_conversion_rate = self.manual_conversion(output_df.ix[sku])
                output_df['Auto ads conversion rate'] = auto_conversion_rate
                output_df['Manual ads conversion rate'] = manual_ad_conversion_rate

            else:
                auto_conversion_rate = 0
                df = output_df.ix[sku]
                manual_ad_conversion_rate = df['1-day Orders Placed (#)'].sum()/(df['Clicks'].sum()+0.001)
                output_df['Auto ads conversion rate'] = auto_conversion_rate
                output_df['Manual ads conversion rate'] = manual_ad_conversion_rate

            output_df.ix[sku].sort_values('Impressions', ascending=False).to_excel(writer, sheet_name=sku)
            days = (self.end - self.start).days
            writer = diagnose.diagnose(writer, sku, output_df.ix[sku].shape, days)

    # 是否还需要根据sku来计算总和？
        return None


    def run_main(self):
        '''
        主函数, 查询周期在60天内
        :return:
        '''
        print "This program process ads data only!"
        print "Searchable store name: HYY, SX, TXHL."
        print "Searchable country code:  DE, ES, FR, IT, UK, JP, CA, US"
        self.store = raw_input("Input store name: ")
        self.country = raw_input("Input country code: ")
        self.start = raw_input("Input start date: ")
        self.end = raw_input("Input end date: ")
        self.store = self.store.upper()
        self.country = self.country.upper()

        self.start, self.end = self.time_process()
        if (self.start, self.end) == (0, 0):
            print "Wrong date format."
            return None

        print ' '
        print 'Loading...'
        print ' '

        # 载入文件
        ad_campaign = self.file_load()

        if ad_campaign.shape == (0, 0):
            print "Loaded file failed. "
            return None

        # 文件处理
        ad_campaign = self.file_process(ad_campaign,)

        print "Data Processing..."

        fname = "../Result/" +self.store + self.country + "_From_" + str(self.start) + "_to_" + str(self.end) + ".xlsx"
        writer = pd.ExcelWriter(fname, engine='xlsxwriter')


        # 在时间断内，统计转化率等信息
        ads_sr = self.time_search(ad_campaign, writer)

        # 对不同sku的产品，统计转化率等信息
        self.sku_search(ad_campaign, writer)


        writer.save()

        print ""
        print "Excel file was generated at " + fname

if __name__ == '__main__':
    A = AmzAdsAnalysis()
    A.run_main()

