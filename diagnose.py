# -*- coding:utf-8 -*-
'''
   作者：陈广
   版本：20170712
   功能介绍：对数据做出诊断。按层级依次为曝光量，CTR， 点击量， CVR；
   其中曝光量以每天200次为阈值，CTR以1%为阈值，点击量以每天1次为阈值，CVR25%为阈值；
   测试数据：days = 30
            Impressions =   9000    9000    9000   9000     9000    6000    4500
            CTR =           5%      5%      5%     1%       0.5%    1%      1%
            Clicks =        450     450     450    90       45      60      45
            CVR =           30%     25%     15%    30%      30%     30%     30%
'''

from __future__ import division

import xlsxwriter


def diagnose(writer, sheet_name, df_shape, days):
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    pass_fmt = workbook.add_format({'bg_color': 'green'})
    failed_fmt = workbook.add_format({'bg_color': 'red'})
    # bold = workbook.add_format({'bold': True})
    nrows, ncols = df_shape

    # Impressions 条件格式设置
    b_col_range = 'B2:B'+str(nrows+1)
    worksheet.conditional_format(b_col_range, {'type': 'cell',
                                         'criteria': '>=',
                                         'value': days*200,
                                         'format': pass_fmt})
    worksheet.conditional_format(b_col_range, {'type': 'cell',
                                         'criteria': '<=',
                                         'value': days*200,
                                         'format': failed_fmt})

    # CTR 条件格式设置
    c_col_range = 'C2:C'+str(nrows+1)
    worksheet.conditional_format(c_col_range, {'type': 'cell',
                                               'criteria': '>=',
                                               'value': 0.01,
                                               'format': pass_fmt})
    worksheet.conditional_format(c_col_range, {'type': 'cell',
                                               'criteria': '<=',
                                               'value': 0.01,
                                               'format': failed_fmt})

    # Clicks 条件格式设置
    d_col_range = 'D2:D' + str(nrows+1)
    worksheet.conditional_format(d_col_range, {'type': 'cell',
                                               'criteria': '>=',
                                               'value': days * 1,
                                               'format': pass_fmt})
    worksheet.conditional_format(d_col_range, {'type': 'cell',
                                               'criteria': '<=',
                                               'value': days * 1,
                                               'format': failed_fmt})

    # Conversion 条件格式设置
    e_col_range = 'E2:E' + str(nrows + 1)
    worksheet.conditional_format(e_col_range, {'type': 'cell',
                                               'criteria': '>=',
                                               'value': 0.25,
                                               'format': pass_fmt})
    worksheet.conditional_format(e_col_range, {'type': 'cell',
                                               'criteria': '<=',
                                               'value': 0.25,
                                               'format': failed_fmt})

    note_range = 'A' + str(nrows + 2)
    worksheet.insert_textbox(note_range, 'Note: Red in the table means unqualified, '
                                                   'and green means qualified.',
                             {'font': {'italic': True}, 'width': 450,  'height': 30})
    return writer



    # data = xlrd.open_workbook(fname)
    # sheets = data.sheet_names()
    # for sheet in sheets:
    #     table = data.sheet_by_name(sheet)
    #     nrows = table.nrows
    #     ncols = table.ncols
    #     for j in range(ncols):
    #         for i in range(nrows):
    #             if (i==0)|(j==0):
    #                 value = table.cell_value(i,j,)
    #                 print value
    #                 #在新表单中写入值
    #         index_value = table.cell_value(0,j,)
    #         if index_value == 'Conversion Rate':
    #             for i in range(nrows)[1:]:
    #                 cell_value = table.cell_value(i,j,)
    #                 print cell_value


# i_threshold_value = 200
# ctr_threshold_value = 0.01
# c_threshold_value = 2
# cvr_threshold_value = 0.25
#
# days = 30
# impressions = 9000
# ctr = 0.05
# clicks = 450
# cvr = 0.3
#
# impression = impressions/days
# click = clicks/days



# if impressions >= i_threshold_value:
#     print "Impressions pass"
#     if ctr >= ctr_threshold_value:
#         print "Click-Through-Rate pass"
#         if click >= c_threshold_value:
#             print "Clicks pass"
#             if cvr >= cvr_threshold_value:
#                 print "Conversion Rate pass"
#             else:
#                 print "Conversion  Rate failed"
#         else:
#             print "Clicks failed"
#     else:
#         print "Cilck-Through-Rate failed"
# else:
#     print "Impressions failed"
