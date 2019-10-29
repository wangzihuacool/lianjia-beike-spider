#!/usr/bin/env python
# coding=utf-8
# author: wl_lw

# lantingshidai的二手房市场均价
SQL_XIAOQU_SUMMARY = """
select 'lanting', date(update_time) as update_time, round(avg(unitprice)) as unitprice, count(*) as house_num, round(avg(lastdays)) as lastdays, sum(followers) as followers
from wh_ershou
where xiaoqu = '广电兰亭时代' and date(update_time) > curdate() - 2
group by date(update_time) order by update_time desc;
"""

# lantingshidai的当天房价情况(不区分大小)
SQL_XIAOQU_DETAIL = """
select date(update_time) as update_time, xiaoqu, zhuangxiu, round(avg(unitprice)) as unitprice, count(*) as house_num, round(avg(lastdays)) as lastdays, sum(followers) as followers from wh_ershou
where xiaoqu = '广电兰亭时代' and date(update_time) > curdate() -1
group by date(update_time), xiaoqu, zhuangxiu
order by unitprice, house_num desc;
"""

# lantingshidai的当天房价情况(区分大小)
SQL_XIAOQU_SIZE_DETAIL = """
select date(update_time), xiaoqu, zhuangxiu, size, round(avg(unitprice)), count(*) as house_num, round(avg(lastdays)) as lastdays, sum(followers) as followers from
(select date(update_time) as update_time, xiaoqu, zhuangxiu, case when size < 80 then 70 when size < 100 and size >= 80 then 90 else 110 end as size, unitprice, lastdays, followers from wh_ershou) w
where xiaoqu = '广电兰亭时代' and date(update_time) > curdate() -1
group by date(update_time), xiaoqu, zhuangxiu, size;
"""

# sixin的二手房市场均价
SQL_AREA_SUMMARY = """
select 'sixin', date(update_time) as update_time, round(avg(unitprice)) as unitprice, count(*) as house_num, round(avg(lastdays)) as lastdays, sum(followers) as followers
from wh_ershou
where area = '四新' and date(update_time) > curdate() - 2
group by date(update_time) order by update_time desc;
"""

# hanyang的二手房市场均价
SQL_DISTRICT_SUMMARY = """
select 'hanyang', date(update_time) as update_time, round(avg(unitprice)) as unitprice, count(*) as house_num, round(avg(lastdays)) as lastdays, sum(followers) as followers
from wh_ershou
where district = '汉阳' and date(update_time) > curdate() - 2
group by date(update_time) order by update_time desc;
"""

# wh整个二手房市场均价
SQL_CITY_SUMMARY = """
select 'wh', date(update_time) as update_time, round(avg(unitprice)) as unitprice, count(*) as house_num, round(avg(lastdays)) as lastdays, sum(followers) as followers
from wh_ershou
where date(update_time) > curdate() - 2
group by date(update_time) order by update_time desc;
"""

