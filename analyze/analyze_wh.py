#!/usr/bin/env python
# coding=utf-8
# author: wl_lw


from lib.utility.mysql_operate import MysqlOperate
from analyze.analyze_sql import *
from env import *
from functools import reduce
import requests
import json

#获取wh二手房价分析情况
class AnalyzeWH(object):
    def __init__(self):
        self.MysqlConn = MysqlOperate(**mysql_db_info)

    def analyze_xiaoqu_summary(self):
        res_xiaoqu_summary = self.MysqlConn.mysql_select(SQL_XIAOQU_SUMMARY)
        return res_xiaoqu_summary

    def analyze_xiaoqu_detail(self):
        res_xiaoqu_detail = self.MysqlConn.mysql_select(SQL_XIAOQU_DETAIL)
        return res_xiaoqu_detail

    def analyze_area_summary(self):
        res_area_summary = self.MysqlConn.mysql_select(SQL_AREA_SUMMARY)
        return res_area_summary

    def analyze_district_summary(self):
        res_district_summary = self.MysqlConn.mysql_select(SQL_DISTRICT_SUMMARY)
        return res_district_summary

    def analyze_city_summary(self):
        res_city_summary = self.MysqlConn.mysql_select(SQL_CITY_SUMMARY)
        return res_city_summary

    def mysql_close(self):
        self.MysqlConn.close()

    # wh二手房分析
    def analyze_wh(self):
        res_xiaoqu_summary = self.analyze_xiaoqu_summary()
        res_xiaoqu_detail = self.analyze_xiaoqu_detail()
        res_area_summary = self.analyze_area_summary()
        res_district_summary = self.analyze_district_summary()
        res_city_summary = self.analyze_city_summary()
        # city info
        city_unitprice = res_city_summary[0][2] if res_city_summary[0][0] == 'wh' else 0
        city_unitprice_lastday = res_city_summary[1][2] if res_city_summary[1][0] == 'wh' else 0
        city_trend = '环比增长 {:.2%}'.format((city_unitprice - city_unitprice_lastday)/city_unitprice_lastday) if city_unitprice >= city_unitprice_lastday else '环比下降 {:.2%}'.format((city_unitprice_lastday - city_unitprice)/city_unitprice_lastday)
        city_info = '武汉二手房市场平均房价{city_unitprice}，'.format(city_unitprice=str(city_unitprice)) + city_trend + '；'
        # district info
        district_unitprice = res_district_summary[0][2] if res_district_summary[0][0] == 'hanyang' else 0
        district_unitprice_lastday = res_district_summary[1][2] if res_district_summary[1][0] == 'hanyang' else 0
        district_trend = '环比增长 {:.2%}'.format((district_unitprice - district_unitprice_lastday) / district_unitprice_lastday) if district_unitprice >= district_unitprice_lastday else '环比下降 {:.2%}'.format((district_unitprice_lastday - district_unitprice) / district_unitprice_lastday)
        district_info = '汉阳平均房价{district_unitprice}，'.format(district_unitprice=str(district_unitprice)) + district_trend + '；'
        # area info
        area_unitprice = res_area_summary[0][2] if res_area_summary[0][0] == 'sixin' else 0
        area_unitprice_lastday = res_area_summary[1][2] if res_area_summary[1][0] == 'sixin' else 0
        area_trend = '环比增长 {:.2%}'.format((area_unitprice - area_unitprice_lastday) / area_unitprice_lastday) if area_unitprice >= area_unitprice_lastday else '环比下降 {:.2%}'.format((area_unitprice_lastday - area_unitprice) / area_unitprice_lastday)
        area_houses = res_area_summary[0][3] if res_area_summary[0][0] == 'sixin' else 0
        area_houses_lastday = res_area_summary[1][3] if res_area_summary[1][0] == 'sixin' else 0
        area_houses_trend = '环比增长 {:.2%}'.format((area_houses - area_houses_lastday) / area_houses_lastday) if area_houses >= area_houses_lastday else '环比下降 {:.2%}'.format((area_houses_lastday - area_houses) / area_houses_lastday)
        area_info = '四新片区平均房价{area_unitprice}，'.format(area_unitprice=str(area_unitprice)) + area_trend + '，' + '二手房待售 {area_houses}套'.format(area_houses=str(area_houses)) + area_houses_trend + '；'
        # xiaoqu info
        xiaoqu_unitprice = res_xiaoqu_summary[0][2] if res_xiaoqu_summary[0][0] == 'lanting' else 0
        xiaoqu_unitprice_lastday = res_xiaoqu_summary[1][2] if res_xiaoqu_summary[1][0] == 'lanting' else 0
        xiaoqu_trend = '环比增长 {:.2%}'.format((xiaoqu_unitprice - xiaoqu_unitprice_lastday) / xiaoqu_unitprice_lastday) if xiaoqu_unitprice >= xiaoqu_unitprice_lastday else '环比下降 {:.2%}'.format((xiaoqu_unitprice_lastday - xiaoqu_unitprice) / xiaoqu_unitprice_lastday)
        xiaoqu_houses = res_xiaoqu_summary[0][3] if res_xiaoqu_summary[0][0] == 'lanting' else 0
        xiaoqu_houses_lastday = res_xiaoqu_summary[1][3] if res_xiaoqu_summary[1][0] == 'lanting' else 0
        xiaoqu_houses_trend = '环比增长 {:.2%}'.format((xiaoqu_houses - xiaoqu_houses_lastday) / xiaoqu_houses_lastday) if xiaoqu_houses >= xiaoqu_houses_lastday else '环比下降 {:.2%}'.format((xiaoqu_houses_lastday - xiaoqu_houses) / xiaoqu_houses_lastday)
        xiaoqu_followers = res_xiaoqu_summary[0][5] if res_xiaoqu_summary[0][0] == 'lanting' else 0
        xiaoqu_last_days = res_xiaoqu_summary[0][4] if res_xiaoqu_summary[0][0] == 'lanting' else 0
        xiaoqu_info = '兰亭时代平均房价{xiaoqu_unitprice}，'.format(xiaoqu_unitprice=str(xiaoqu_unitprice)) + xiaoqu_trend + '，' + '二手房待售 {xiaoqu_houses}套，'.format(xiaoqu_houses=str(xiaoqu_houses)) + xiaoqu_houses_trend + '，' + '关注量{xiaoqu_followers}，'.format(xiaoqu_followers=str(xiaoqu_followers)) + '平均滞售{xiaoqu_last_days}天;'.format(xiaoqu_last_days=str(xiaoqu_last_days))
        # summary info
        summary_info = city_info + district_info + area_info + xiaoqu_info

        # xiaoqu detail
        houses_list = [xiaoqu_detail[4] for xiaoqu_detail in res_xiaoqu_detail]
        houses_total = reduce(lambda x, y: x + y, houses_list)
        maopi_record = list(filter(lambda x: x[2] == '毛坯', res_xiaoqu_detail))
        xiaoqu_maopi_price = maopi_record[0][3] if maopi_record else 0
        xiaoqu_maopi_houses = maopi_record[0][4] if maopi_record else 0
        xiaoqu_maopi_followers = maopi_record[0][6] if maopi_record else 0
        xiaoqu_maopi_days = maopi_record[0][5] if maopi_record else 0
        jingzhuang_record = list(filter(lambda x: x[2] == '精装', res_xiaoqu_detail))
        xiaoqu_jingzhuang_price = jingzhuang_record[0][3] if jingzhuang_record else 0
        xiaoqu_jingzhuang_houses = jingzhuang_record[0][4] if jingzhuang_record else 0
        xiaoqu_jingzhuang_followers = jingzhuang_record[0][6] if jingzhuang_record else 0
        xiaoqu_jingzhuang_days = jingzhuang_record[0][5] if jingzhuang_record else 0
        xiaoqu_others_houses = houses_total - xiaoqu_maopi_houses - xiaoqu_jingzhuang_houses
        maopi_percent = '占比{:.2%}，'.format(xiaoqu_maopi_houses/houses_total)
        jingzhuang_percent = '占比{:.2%}，'.format(xiaoqu_jingzhuang_houses / houses_total)
        others_percent = '占比{:.2%}.'.format(xiaoqu_others_houses / houses_total)
        maopi_info = '兰亭时代毛坯房{maopi}套，'.format(maopi=str(xiaoqu_maopi_houses)) + maopi_percent + '均价{price}，'.format(price=xiaoqu_maopi_price) + '今日关注量{followers}，'.format(followers=str(xiaoqu_maopi_followers)) + '平均滞售{days}天'.format(days=str(xiaoqu_maopi_days))
        jingzhuang_info = '精装房{maopi}套，'.format(maopi=str(xiaoqu_jingzhuang_houses)) + jingzhuang_percent + '均价{price}，'.format(price=xiaoqu_jingzhuang_price) + '今日关注量{followers}，'.format(followers=str(xiaoqu_jingzhuang_followers)) + '平均滞售{days}天'.format(days=str(xiaoqu_jingzhuang_days))
        qita_info = '其他房型{maopi}套，'.format(maopi=str(xiaoqu_others_houses)) + others_percent
        detail_info = maopi_info + '；' + jingzhuang_info + '；' + qita_info

        # 最终
        update_time = res_city_summary[0][1] if res_city_summary[0] else res_xiaoqu_detail[0][0]
        final_info = str(update_time) + ' ' + summary_info + '\n' + detail_info
        return final_info

    # 当日数据推送给boybean
    def push_data(self, information):
        d = {'wh_ershou_info': information}
        d = json.dumps(d)
        r = requests.post('http://www.boybean.cn/wh_ershou', data=d)
        # print(r.status_code)
        # print(r.text)
        return r.status_code


if __name__ == "__main__":
    wh_data = AnalyzeWH()
    message = wh_data.analyze_wh()
    print(message)
    status = wh_data.push_data(message)
    print(status)



