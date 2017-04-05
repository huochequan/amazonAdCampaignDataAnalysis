# -*-　coding: utf-8 -*-

'''
    作者： 陈广
    时间：2017-4-5
    版本号： 20170405
    程序简介：读入亚马逊各个站点的广告活动数据以及销售数据，
    对其某个时段的订单数，点击量，销售量，销售额，ACOS，
    转化率等进行统计计算。最后在Result文件夹输出excel文件。
'''

from __future__ import division
from dateutil.parser import parse
from datetime import timedelta
from pandas import Series, DataFrame
import pandas as pd
import os


class AmzAdsAnalysis:
    def __init__(self):
        self.start = None
        self.end = None
        self.country = None

    def time_process(self):
        '''
        输入时间，返回开始和结束时间
        :return: (start_date, end_date)
        '''
        # 将字符串转换为时间戳
        start_date = parse(self.start).date()
        end_date = parse(self.end).date()
        # 输入时间值判断
        if ((start_date > end_date) | (start_date < parse('20170301').date()) | (end_date > parse('20170319').date())):
            start_date = 0
            end_date = 0

        return start_date, end_date


    def file_load(self, datatype):
        '''
        sales_dict和ads_dict 表示国家对应的广告数据和销售数据的文件目录

        datatype= True，打开广告数据. False, 打开销售数据

        start, end传入时间，可为None。目前暂时用于读取销售数据用。

        读销售数据原理：用os.listdir找到数据月份文件夹（如：2017.03），
        根据时间段与文件匹配，读取该时间段内的数据。

        函数返回DataFrame对象

        '''

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
            path = ads_dict[self.country]
            files = os.listdir(path)
            for files_ in files:
                if os.path.isdir(path + files_):  # 找到月份文件夹
                    inner_path = path + files_
                    file_name = "ADs_SX" + self.country + "_" + str(self.end.year) + "-" + str(self.end.month) + "-" + str(self.end.day) + ".txt"
                    if self.country == "JP":
                        ad_campaign = pd.read_table(inner_path + "/" + file_name, sep='\t', encoding='Shift-JIS')
                    else:
                        ad_campaign = pd.read_table(inner_path + "/" + file_name, sep='\t', encoding='utf-8')

            return ad_campaign
        else:
            sales_df = DataFrame()
            path = sales_dict[self.country]
            files = os.listdir(path)
            for files_ in files:
                if os.path.isdir(path + files_):  # 找到月份文件夹
                    inner_path = path + files_
                    delta = (self.end - self.start).days
                    for i in range(delta + 1):
                        date = (self.start + timedelta(days=i)).isoformat()
                        ipath = inner_path + '/' + 'SX' + self.country + '-' + date[2:4] + '-' + date[6:7] + '-' + date[
                                                                                                              8:10] + '.csv'
                        df = pd.read_csv(ipath, encoding='utf-8')
                        sales_df = pd.concat([sales_df, df])
            return sales_df


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
        elif ((self.country == 'CA') | (self.country == 'USA')):
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


    def data_sum(self, grouped):
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


    def manual_conversion(self, df):
        '''
        计算手动广告转换率
        :param ad_campaign, sku:
        :return: manual_ad_conversion_rate
        '''

        df_clicks = df['Clicks'].sum()
        df_orders = df['1-day Orders Placed (#)'].sum()

        manual_ad_clicks = df_clicks - df.ix["*", "Clicks"]

        if (manual_ad_clicks == 0):
            return 0

        manual_ad_orders = df_orders - df.ix["*", "1-day Orders Placed (#)"]
        manual_ad_conversion_rate = manual_ad_orders / manual_ad_clicks
        return manual_ad_conversion_rate


    def sales_data(self, writer):
        '''
        计算销售数据的转化率信息
        :param writer:
        :return:
        '''

        print " "
        print "Sales Data"
        activities = self.file_load(datatype=False)

        activities.columns = ['(Father) ASIN', '(Son) ASIN', '商品名称', 'Total clicks', '买家访问次数百分比',
                              '买家浏览次数', '页面浏览次数百分比', '购买按钮页面浏览率', 'Total ordered', '订单商品数量转化率',
                              'Total ordered product sales', '订单商品种类']

        def get_sales(string):
            string = string.replace(',', '').replace('US', '').replace('CA', "")
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

        # 统计ASIN码的数据
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

        output_df.to_excel(writer, sheet_name='(Son)ASIN')

        return output_sr


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

        output_dataframe.sort_values('Conversion Rate', ascending=False).to_excel(writer, sheet_name='ADs_Campaign')

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
                manual_ad_conversion_rate = self.manual_conversion(output_df.ix[sku])
                output_df['Auto ads conversion rate'] = auto_conversion_rate
                output_df['Manual ads conversion rate'] = manual_ad_conversion_rate

            output_df.ix[sku].sort_values('Conversion Rate', ascending=False).to_excel(writer, sheet_name=sku)

    # 是否还需要根据sku来计算总和？
        return None


    def run_main(self):
        '''
        主函数, 查询周期在60天内
        :return:
        '''
        print "Searchable store code: SX"
        print "Searchable country code:  DE, ES, FR, IT, UK, JP, CA, USA"
        print "Searchable date period: 2017/03/01 - 2017/03/19"
        self.country = raw_input("Input country code: ")
        self.start = raw_input("Input start date: ")
        self.end = raw_input("Input end date: ")
        self.country = self.country.upper()

        self.start, self.end = self.time_process()
        if (self.start, self.end) == (0, 0):
            print "Wrong date format."
            return None

        print ' '
        print 'Loading...'
        print ' '

        # 载入文件
        ad_campaign = self.file_load(datatype=True)

        # 文件处理
        ad_campaign = self.file_process(ad_campaign,)

        print "Data Processing..."

        fname = "../Result/" + self.country + "_From_" + str(self.start) + "_to_" + str(self.end) + ".xlsx"
        writer = pd.ExcelWriter(fname, engine='xlsxwriter')


        # 在时间断内，统计转化率等信息
        ads_sr = self.time_search(ad_campaign, writer)

        # 对销售数据， 统计转化率等信息
        sales_sr = self.sales_data(writer)

        total_acos = float(ads_sr['Total Spend'])/float(sales_sr['Total ordered product sales'])
        total_acos = Series(total_acos, index=['Total ACOS'])
        # 合并数据
        total_analysis = pd.concat([ads_sr, sales_sr, total_acos])
        total_analysis = total_analysis.to_frame('Result')

        total_analysis.to_excel(writer, sheet_name="Totality_Data")

        # 对不同sku的产品，统计转化率等信息
        self.sku_search(ad_campaign, writer)

        writer.save()

        print ""
        print "Excel file was generated at " + fname

if __name__ == '__main__':
    A = AmzAdsAnalysis()
    A.run_main()

