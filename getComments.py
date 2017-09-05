import  pymysql

class getComments():

    def __init__(self):
        pass

    def save_comm(self):
        conn = pymysql.connect(
                host='127.0.0.1',
                port=3306,
                user='spider',
                password='123456',
                db='spider',
                charset='utf8mb4')  # connect to mysql

        cur = conn.cursor()
        sql = "INSERT INTO `comments`(`content`, `star`, `hotel_member_lv`, `date`, `ht_id`, `page_num`) VALUES ('😁😁不可否认环境歪瑞歪瑞颇费特！别墅区总体环境也很好！吃完晚饭还可以到路边散步！花草树木无一不是一处风景！重点是老板人相当好相当好相当好！重要的话说三遍！不仅接送我们去吃喝玩乐！还邀请我们一起吃晚饭！而且还是宜良的特产烤鸭！专门去买的喔！还有窝窝头次！味道简直了！总之真的很满意这次的住宿！本来想自驾！后来还是选择了坐车！冥冥中让我们遇见曹总辣么好的人！多了个朋友！还和我们说哪里好玩！什么东西好次！对于我这个吃货来说！吃大过天啊！简直就是免费的导游啊！还有做的饭菜味道简直不要太好次！表示已经次的很撑！很满意这次的旅游！高尔夫球场任你玩！自拍任你360度无死角！多不说！上图\n', '100%', 'vip1', '2017-05-08', 1, 1)";
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

if __name__ == '__main__':
    c = getComments()
    c.save_comm()
