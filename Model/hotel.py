import pymysql
import redis as redis
from selenium import webdriver
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from lxml import etree
import redis
import random
from pymysql.err import ProgrammingError

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
        sql = "SELECT id, url FROM `hotel_info` WHERE `web_id`= 1 and id > 15 ORDER BY `id` ASC "
        self.__cur.execute(sql)
        self.__conn.commit()
        return self.__cur.fetchall()

    """get comments of hotel"""
    def getComment(self):
        htinfo = self.getHotelInfo()

        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])  # 屏蔽掉浏览器的认证错误
        # options.add_experimental_option('prefs', {"profile.managed_default_content_settings.images": 2})
        driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options)
        for i in range(len(htinfo)):

            try:
                ht_id = htinfo[i][0]
                driver.get("http://" + htinfo[i][1])
                time.sleep(10)
                comment_ele = driver.find_element_by_xpath("//ul[@class='J-nav-tabs nav-tabs--normal cf log-mod-viewed']//li[@data-target='.J-poi-comment']/a")
                comment_count = str(comment_ele.text).strip("住客点评").strip(" (").strip(")")
                comment_ele.click()         # jump to the comment page

                print("len:" + str(len(comment_count)))

                if len(comment_count) is not 0:         # hotel comment count not 0
                    print("is not 0")
                    next_page = True
                    # driver.find_element_by_xpath("//select[@class='J-filter-ordertype ui-select-small']//option[@value='time']").click() # comment sorted by time
                    while next_page is True:
                        time.sleep(random.randint(10, 18))
                        selector = etree.HTML(driver.page_source)
                        comments = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//p[@class='content']//text()")  # comments content
                        star = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//span[@class='common-rating']//span//@style")
                        member_lev = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//span[@class='growth-info']//i//@title")
                        date = selector.xpath("//ul[@class='J-rate-list']//li[@class='J-ratelist-item rate-list__item cf']//div[@class='info cf']//span[@class='time']//text()")
                        if(int(comment_count) > 10):
                            page_num = selector.xpath("//div[@class='paginator J-rate-paginator']//li[@class='current']//span//@data-index")[0]       # current page num
                        else:
                            page_num = 1

                        print("save:" + str(ht_id)+'_'+str(page_num))
                        if self.__redis.sismember('is_saved_comments', str(ht_id)+'_'+str(page_num)) == 0:
                            for x in range(len(comments)):
                                self.save_comments_info(content=str(comments[x]).strip('\n').strip(" ").replace('\n', ''), star=str(star[x]).strip('width:'), hotel_member_lv=str(member_lev[x]).strip("等级"), date=date[x], ht_id=ht_id, page_num=page_num)
                            self.__redis.sadd('is_saved_comments', str(ht_id)+'_'+str(page_num))            # wether saved this page comment
                        else:
                            print("this page already saved")

                        next_page__att = selector.xpath("//div[@class='paginator J-rate-paginator']//li[@class='next']//i//@class")

                        print(comment_count, next_page__att)
                        if int(comment_count) < 10 or next_page__att[0] == 'tri disable':   # only one page, next page cannot click
                            print("there is no next page, to next hotel")
                            next_page = False
                        else:
                            print("click the next page")
                            driver.find_element_by_xpath("//div[@class='paginator J-rate-paginator']//li[@class='next']//i[@class='tri']").click()

            except IndexError as ie_rr:
                continue
            except NoSuchElementException as no_ele_err:
                print(no_ele_err)
                continue
            except WebDriverException as web_err:
                print(web_err)
                continue
        driver.close()

    def save_comments_info(self, content, star, hotel_member_lv, date, ht_id, page_num):
        sql = "INSERT INTO `comments`(`content`,`star`,`hotel_member_lv`,`date`, `ht_id`, `page_num`) VALUES ('%s','%s','%s','%s', %d, %d)"
        data = (content, star, hotel_member_lv, date, int(ht_id), int(page_num))
        print(data)
        try:
            self.__cur.execute(sql%data)
            self.__conn.commit()
        except ProgrammingError as pe:
            pass

    def __del__(self):
        self.close()


if __name__ == '__main__':
    hotel = Hotel()
    # hotel.getHotelInfo()
    hotel.getComment()
