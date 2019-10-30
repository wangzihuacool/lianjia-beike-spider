#!/usr/bin/env python
# coding=utf-8
# author: wl_lw
# 分析制定城市房价信息

from analyze.analyze_wh import AnalyzeWH

if __name__ == "__main__":
    wh_data = AnalyzeWH()
    message = wh_data.analyze_wh()
    print(message)
    status = wh_data.push_data(message)
    print(status)