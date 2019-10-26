#!/usr/bin/env python
# coding=utf-8
# author: zengyuetian
# 此代码仅供学习与交流，请勿用于商业用途。
# 二手房信息的数据结构

import re


# 丰富爬取的信息，同时增加方法直接插入mysql数据库
class ErShou(object):
    def __init__(self, district, area, xiaoqu, name, price, unitprice, desc, followinfo, pic):
        self.district = district
        self.area = area
        self.xiaoqu = xiaoqu
        self.price = price
        self.name = name
        self.desc = desc
        self.pic = pic
        self.unitprice = unitprice
        self.followinfo = followinfo

    def text(self):
        return self.district + "," + \
                self.area + "," + \
                self.xiaoqu + "," + \
                self.name + "," + \
                self.price + "," + \
                self.unitprice + "," + \
                self.desc + "," + \
                self.followinfo + "," + \
                self.pic

    # 数据清洗
    def handle_info(self):
        district = self.district
        area = self.area
        xiaoqu = self.xiaoqu
        price = int(float(re.findall(r"\d+\.?\d*", self.price)[0]) * 10000)
        unitprice = int(re.findall(r"\d+\.?\d*", self.unitprice)[0])
        size = float(re.findall(r"\d+\.?\d*", self.desc.split('|')[1].strip())[0])
        zhuangxiu = self.desc.split('|')[3].strip()
        louceng = self.desc.split('|')[4].strip()[0] if self.desc.split('|')[4].strip() else None
        tmp_list = self.followinfo.split('/')
        try:
            followers = int(re.findall(r"\d+\.?\d*", tmp_list[0])[0]) if tmp_list[0] else None
        except:
            followers = None
        try:
            lastdays = int(re.findall(r"\d+\.?\d*", tmp_list[1])[0]) if tmp_list[1] else None
        except:
            lastdays = None
        title = self.name
        desc = self.desc
        pic = self.pic
        house_info = (district, area, xiaoqu, price, unitprice, size, zhuangxiu, louceng, followers, lastdays, title, desc, pic)
        return house_info
