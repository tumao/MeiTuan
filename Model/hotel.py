import pymysql
from selenium import webdriver
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from lxml import etree
import redis
import random
from pymysql.err import ProgrammingError
import json

class Hotel():

    def __init__(self):
        self.connect()

        self.__conn = self.connect()                # connect to mysql and get cursor
        if self.__conn:
            self.__cur = self.__conn.cursor()

        self.__redis = self.connect_redis()         # connect to redis


    def connect(self):
        conn = False
        try:
            conn = pymysql.connect(
                host='127.0.0.1',
                port=3306,
                user='spider',
                password='123456',
                db='spider',
                charset='utf8mb4')  # connect to mysql
        except Exception as e:
            conn = False
            print(e)
        return conn

    """close the mysql"""
    def close(self):
        if self.__conn:
            try:
                if type(self.__cursor) is 'object':
                    self.__cursor.close()
                if type(self.__conn) is 'object':
                    self.__conn.close()
            except Exception as e:
                print(self)

    """connect to redis"""
    def connect_redis(self):
        return redis.Redis(host='127.0.0.1', port=6379, db=1)

    """get hotel info"""
    def getHotelInfo(self):
        print(self.__redis.get("cur_spid_ht_id"))

        if self.__redis.get("cur_spid_ht_id") is not None:
            cur_spid_ht_id = int(self.__redis.get("cur_spid_ht_id"))
        else:
            cur_spid_ht_id = 912

        sql = "SELECT id, url FROM `hotel_info` WHERE `web_id`= 1 and id > %d ORDER BY `id` ASC "
        self.__cur.execute(sql%(cur_spid_ht_id))
        self.__conn.commit()
        return self.__cur.fetchall()

    """get comments of hotel"""
    def getComment(self):
        htinfo = self.getHotelInfo()

        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])  # 屏蔽掉浏览器的认证错误
        options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36')
        # options.add_experimental_option('prefs', {"profile.managed_default_content_settings.images": 2})
        driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options)
        for i in range(len(htinfo)):

            try:
                ht_id = htinfo[i][0]
                self.__redis.set('cur_spid_ht_id', ht_id)
                driver.get("http://" + htinfo[i][1])
                time.sleep(10)
                # comment_ele = driver.find_element_by_xpath("//ul[@class='J-nav-tabs nav-tabs--normal cf log-mod-viewed']//li[@data-target='.J-poi-comment']/a")
                comment_ele = driver.find_element_by_xpath("//ul[@class='nav-tabs clearfix bgw']//a[@href='#comment']")
                comment_count = str(comment_ele.text).strip("住客点评").strip(" (").strip(")")
                comment_ele.click()         # jump to the comment page

                print("len:" + str(len(comment_count)))

                if len(comment_count) is not 0:         # hotel comment count not 0
                    print("is not 0")
                    next_page = True
                    # driver.find_element_by_xpath("//select[@class='J-filter-ordertype ui-select-small']//option[@value='time']").click() # comment sorted by time
                    while next_page is True:
                        wait_time = random.randint(15, 25)
                        # wait_time = 5
                        print("此处停留"+str(wait_time)+"s")
                        time.sleep(wait_time)
                        selector = etree.HTML(driver.page_source)
                        # comments = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//p[@class='content']//text()")  # comments content
                        comments = selector.xpath("//div[@class='ratelist-content clearfix']//ul//li[@class='rate-list-item clearfix']//p[@class='content']//text()")
                        # star = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//span[@class='common-rating']//span//@style")
                        star = selector.xpath("//div[@class='ratelist-content clearfix']//ul//li[@class='rate-list-item clearfix']//div[@class='rate-status']//span//@style")
                        # member_lev = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//span[@class='growth-info']//i//@title")
                        # date = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//div[@class='info cf']//span[@class='time']//text()")
                        date = selector.xpath("//div[@class='ratelist-content clearfix']//ul//li[@class='rate-list-item clearfix']//span[@class='time pull-right']//text()")
                        if(int(comment_count) > 10):
                            page_num = selector.xpath("//div[@class='paginator-wrapper']//li[@class='current']//span//text()")[0]       # current page num
                        else:
                            page_num = 1

                        print("save:" + str(ht_id)+'_'+str(page_num))
                        if self.__redis.sismember('is_saved_comments', str(ht_id)+'_'+str(page_num)) == 0:
                            self.save_source_code(ht_id=ht_id, page_id=int(page_num), source_code=driver.page_source)
                            for x in range(len(comments)):
                                self.save_comments_info(content=str(comments[x]).strip('\n').strip(" ").replace('\n', ''), star=str(star[x]).strip('width:'), hotel_member_lv=str("000-000"), date=date[x], ht_id=ht_id, page_num=page_num)
                            self.__redis.sadd('is_saved_comments', str(ht_id)+'_'+str(page_num))            # wether saved this page comment
                        else:
                            print("this page already saved")

                        self.__redis.incrby(str(ht_id)+'_'+str(page_num), 1)
                        if int(self.__redis.get(str(ht_id)+'_'+str(page_num))) > 3:      # 如果当前页面无法翻页，卡住时，则刷新当前页面
                            driver.refresh()

                        next_page__att = selector.xpath("//div[@class='paginator-wrapper']//li[last()]//@class")

                        print(comment_count, next_page__att)
                        if int(comment_count) < 10 or next_page__att[0] == 'disabled next':   # only one page, next page cannot click
                            print("there is no next page, to next hotel")
                            next_page = False
                        else:
                            print("click the next page")
                            driver.find_element_by_xpath("//div[@class='paginator-wrapper']//li[@class=' next']//a").click()

            except IndexError as ie_rr:
                pass
            except NoSuchElementException as no_ele_err:
                print(no_ele_err)
                pass
            except WebDriverException as web_err:
                print(web_err)
                pass
        driver.close()

    def save_comments_info(self, content, star, hotel_member_lv, date, ht_id, page_num):
        sql = "INSERT INTO `comments`(`content`,`star`,`hotel_member_lv`,`date`, `ht_id`, `page_num`) VALUES ('%s','%s','%s','%s', %d, %d)"
        data = (content, star, hotel_member_lv, date, int(ht_id), int(page_num))
        # print(data)
        try:
            self.__cur.execute(sql%data)
            self.__conn.commit()
        except ProgrammingError as pe:
            pass

    """save source code to redis"""
    def save_source_code(self, ht_id, page_id, source_code):
        source_code = str(source_code).replace("<script>","|script|").replace("<\script>", "|script|")
        self.__redis.hset("source_code_comment_"+str(ht_id), str(page_id), json.dumps(source_code))

    def __del__(self):
        self.close()


if __name__ == '__main__':
    hotel = Hotel()
    # hotel.getHotelInfo()
    hotel.getComment()
