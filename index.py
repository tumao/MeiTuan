import requests
from selenium import webdriver
import time
import pymysql
import redis
from lxml import etree
import math
from selenium.common.exceptions import NoSuchElementException
import json

headers = {
    "Accept":"text/html,application/xhtml+xml,application/xml;",
    "Accept-Encoding":"gzip",
    "Accept-Language":"zh-CN,zh;q=0.8",
    "Referer":"http://www.example.com/",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"
    }

hoteltype = {'经济型酒店':"20036",
        "主题酒店":"20037",
        "商务酒店":"20038",
        "公寓":"20039",
        "豪华酒店":"20040",
        "客栈":"20041",
        "青年旅社":"20042",
        "度假酒店":"20043",
        "别墅":"20044",
        "农家院":"20045"}
conn = pymysql.connect(host='127.0.0.1', port=3306, user='spider', password='123456', db='spider', charset='utf8')   # connect to mysql
cursor = conn.cursor()
redis = redis.Redis(host='127.0.0.1', port=6379, db=1)

sql = "INSERT INTO `hotel_type`(`typename`, `code`) VALUES('%s', '%s')"
memberedSql = "SELECT `typename`, `code`, `id` FROM `hotel_type` order by `id`"
cursor.execute(memberedSql)
conn.commit()
alltype = cursor.fetchall()

for type in alltype:              # save existed type to redis
    redis.sadd('hoteltype', type[0])

for x in hoteltype.keys():
    if redis.sismember('hoteltype', x) == 0:              # is remember the hotel
        redis.sadd('hoteltype', x)
        data = (x, str(hoteltype[x]))
        cursor.execute(sql%data)
        conn.commit()
        redis.sadd('hoteltype', x)

hotellist = "http://hotel.meituan.com/kunming?mtt=1.index%2Ffloornew.nc.33.j6x8ynqd#ci=2017-08-29&co=2017-08-30&sort=&w=&attrs=20022:"

for x in alltype:
    tempurl = hotellist+str(x[1])
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])  # 屏蔽掉浏览器的认证错误
    options.add_experimental_option('prefs', {"profile.managed_default_content_settings.images": 2})  # 使chrome 浏览器不加载图片信息
    driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options)
    driver.get(url=tempurl)
    time.sleep(10)
    hotelcount = driver.find_element_by_xpath("//div[@id='selected-filter']//i").text
    # print("总页数："+str(math.ceil(int(hotelcount)/20)))
    for i in range(math.ceil(int(hotelcount)/20)):
        try:
            pageurl = tempurl + "&page="+str(i+1)
            # print(pageurl)
            driver.refresh()
            driver.get(pageurl)

            time.sleep(10)
            web_id = 1
            selector = etree.HTML(driver.page_source)
            htname = selector.xpath("//div[@id='hotel-list-wrapper']/div[@class='hotel']//div[@class='intro']/a/text()")    # 酒店名称
            htids = selector.xpath("//div[@id='hotel-list-wrapper']/div[@class='hotel']//@data-poiid")      #酒店id
            site = selector.xpath("//div[@id='hotel-list-wrapper']/div[@class='hotel']//div[@class='intro']/span[1]/text()")
            score = selector.xpath("//div[@id='hotel-list-wrapper']/div[@class='hotel']//div[@class='rating']//div[@class='score']/a/span[1]/text()")
            url = selector.xpath("//div[@id='hotel-list-wrapper']/div[@class='hotel']//div[@class='intro']/a//@href")    # 酒店名称
            typeid = x[2]
            # print(len(site))
            # print(len(score))
            # print(len(url))
            # print(len(htids))
            # print(len(htname))
            print(typeid)
            for j in range(len(site)):
                if redis.sismember('ht_wb_id', int(htids[j])) == 0 :        # 如果数据库中没有保存则保存
                    redis.sadd('ht_wb_id', int(htids[j]))
                    sql = "INSERT INTO `hotel_info`(`web_id`, `ht_id`, `ht_name`, `star`, `site_info`, `type_id`, `url`) VALUES(%d, %d, '%s', '%s', '%s', %d, '%s')"
                    htdata = (web_id, int(htids[j]), htname[j], str(score[j]), site[j], typeid, str(url[j]).strip("//"))
                    cursor.execute(sql%htdata)
                    conn.commit()
        except IndexError as ie:
            continue
        except NoSuchElementException as no_ele_err:
            continue
    driver.close()


cursor.close()
conn.close()