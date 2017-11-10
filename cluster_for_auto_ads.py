# -*- coding:utf8 -*-
'''
 程序：自动广告报告关键词聚类分析
 作者：陈广
 版本：20171110
'''

import pandas as pd
from sklearn import preprocessing
from sklearn.cluster import KMeans



data = pd.read_table("F:\\PycharmFile\\Data\\ALLDE\\EU\\ADS\\0730\\AUTOADS_ALLDEDE_2017-7-30.txt")
data.columns = [
    "Campaign_Name",
    "Ad Group_Name",
    "Customer_Search_Term",
    "Keyword",
    "Match_Type",
    "First_Day_of_Impression",
    "Last_Day_of_Impression",
    "Impressions",
    "Clicks",
    "CTR",
    "Total_Spend",
    "Average_CPC",
    "ACoS",
    "Currency",
    "Orders_placed_within_1_week_of_a_click",
    "Product_Sales_within_1_week_of_a_click",
    "Conversion_Rate_within_1_week_of_a_click",
    "Same_SKU_units_Ordered_within_1_week_of_click",
    "Other_SKU_units_Ordered_within_1_week_of_click",
    "Same_SKU_units_Product_Sales_within_1_week_of_click",
    "Other_SKU_units_Product_Sales_within_1_week_of_click"
]

def rep(string):
    return string.replace(',', '.')

data['Total_Spend'] = data['Total_Spend'].apply(rep).apply(float)
data['Product_Sales_within_1_week_of_a_click'] = data['Product_Sales_within_1_week_of_a_click'].apply(rep).apply(float)

data['First_Day_of_Impression'] = pd.to_datetime(data['First_Day_of_Impression'], dayfirst=True)
data['Last_Day_of_Impression'] = pd.to_datetime(data['First_Day_of_Impression'], dayfirst=True)

data["period"] = data["Last_Day_of_Impression"] - data["First_Day_of_Impression"]


def timedelta2int(timedel):
    return timedel.days
data["period"] = data["period"].apply(timedelta2int)

data1 = data[data["Keyword"] != "*"]

x = data1[["Impressions",
           "Clicks",
           "Total_Spend",
           "Orders_placed_within_1_week_of_a_click",
           "Product_Sales_within_1_week_of_a_click",
           "period"]]

x_scaled = preprocessing.StandardScaler().fit(x)
kmeans = KMeans(n_clusters=3, random_state=0).fit(x_scaled.transform(x))

data1["label"] = kmeans.labels_

data1.to_csv("cluster_result1.csv")

