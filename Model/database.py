import pymysql
import redis


class database:
    DB = 'database'
    Host = '127.0.0.1'
    User = 'root'
    Passwd = '123456'
    Charset = 'utf-8'
    Port = '3306'

    conn = ''
    cursor = ''

    def __init__(self):
        # # redis = redis.Redis(host='127.0.0.1', port=6379, db=1)
        # if dbname is not None:
        #     self.dbname = dbname
        # else:
        #     self.dbname = self.DB
        self.__conn = self.connect()

        if self.__conn:
            self.__cursor = self.__conn.cursor()

    """connect to the mysql """
    def connect(self):
        conn = False
        try:
            conn = pymysql.connect(
                host='127.0.0.1',
                port=3306,
                user='spider',
                password='123456',
                db='spider',
                charset='utf8')  # connect to mysql
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
