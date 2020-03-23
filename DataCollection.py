# *************引用python自带库*************
from collections import Counter
import datetime
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from time import sleep
import logging
# ******************************************

# ***************引用第三方库***************
import psycopg2
# ******************************************

# ***************引用定义的库***************
from HslCommunication import SiemensS7Net
from HslCommunication import SiemensPLCS
# ******************************************

LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(pathname)s %(message)s "  # 配置输出日志格式
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '  # 配置输出时间的格式，注意月份和天数不要搞乱了
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    filename=r"d:\andon\logfile.log"  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                    )


class DataCollection:
    def __init__(self):
        self.database = "ytwebserver"
        self.user = "ytwebserver"
        self.password = "it-profi"
        self.host = 'localhost'
        self.port = "5432"

    def __get_connect(self):
        # 连接数据库
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host,
                                     port=self.port)
        # 获取游标
        cur = self.conn.cursor()
        return cur

    def insert_query(self, sql):
        cur = self.__get_connect()
        cur.execute(sql)
        self.conn.commit()
        cur.close()
        self.conn.close()

    def select_query(self, sql):
        cur = self.__get_connect()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        self.conn.close()
        return rows

    # 连接PLC失败时，调用此函数发送邮件至相关人员
    def plc_connect_failed(self, values):
        print('进入plc_connect_failed')
        logging.info('进入plc_connect_failed')
        message = ''  # 要发送的邮件数据
        l = len(values)
        # 获取SQL搜索条件
        s = ''
        for i in range(l):
            if i < l - 1:  # 如果不是最后一位，在后面添加or关键字
                s += "ip=%s or " % values[i]
            else:  # 如果是最后一位
                s += "ip=%s" % values[i]
        sql = "SELECT DISTINCT project,production_line,ip FROM andon_menu WHERE " + s  # 根据PLC的ip查找项目流水线
        rows = self.select_query(sql)
        for row in rows:
            message += "%s%s流水线的PLC连接失败！PLC的IP为:%s\n" % row
        self.send_email(message)
    
    # 查寻此时此刻正在生产的所有项目
    def find_mps(self):
        print("进入find_mps")
        logging.info('进入find_mps')
        dic_id = {}  # 存储此时此刻生产中的项目(key:value对应andon_mps.id:andon_menu.id)
        dic_ip_menu = {}  # 存储dic_id中andon_menu.id对应的PLC的ip(key:value对应andon_menu.id:andon_menu.id)
        dic_ip_mps = {}  # 存储dic_id中andon_mps.id对应的PLC的ip(key:value对应andon_mps.id:andon_menu.id)
        dic_ip_now = {}  # 存储dic_ip_mps去重后的项目 
        list_items = []  # 存储所有项目的名称
        list_items_duplicate = []  # 存储list_items中PLC的ip重复的项
        str_duplicate_items = ''  # 把list_items_duplicate字符串化，发送邮件给相关人员
        
        # 获取当前系统时间
        t_now = datetime.datetime.now()
        
        # SQL语句：查询当前时间正在生产的项目的id（即andon_menu表中的id）以及计划表中的id（即andon_mps表中的id--存入andon_history表时要用到)
        sql = "SELECT id,menu_info_id FROM andon_mps WHERE start_time<'%s' and end_time>'%s'" % (t_now, t_now)
        # print('SQL', sql)
        # 获取全部查询集
        rows = self.select_query(sql)
        # print('rows=', rows)
        
        # 获取所有正在生产的项目的id（key:value对应andon_mps.id:andon_menu.id）
        dic_id = {x[0]: x[1] for x in rows}
        # print("满足当前条件的所有项目(andon_mps.id:andon_menu.id):", dic_id)

        if len(dic_id) > 0:  # 如果存在数据
            # 获取andon_menu的id
            list_id_menu = list(set(dic_id.values()))  # 去除重复项 在'获取SQL搜索条件'那一步使用 set()可以去除重复项
            l = len(list_id_menu)
            # 获取SQL搜索条件
            s = ''
            for i in range(l):
                if i < l - 1:  # 如果不是最后一位，在后面添加or关键字
                    s += "id=%s or " % list_id_menu[i]
                else:  # 如果是最后一位
                    s += "id=%s" % list_id_menu[i]
            # SQL语句:获取dic_id中去重后的andon_menu.id对应的项目
            sql = "SELECT id,project,production_line,product,ip FROM andon_menu WHERE " + s
            # print('SQL:', sql)
            rows = self.select_query(sql)
            for row in rows:
                dic_ip_menu[row[0]] = row[4]  # 将andon_menu.id 与 plc的IP对应起来
                list_items.append((row[1]+row[2]+row[3], row[4]))
            # 将andon_mps.id 与 PLC的IP对应起来(即：将andon_menu.id替换为对应的andon_menu.ip，包括重复的andon_menu.id项）    
            dic_ip_mps = {key: dic_ip_menu[value] for key, value in dic_id.items()}

        if len(dic_ip_mps.values()) > len(set(dic_ip_mps.values())):  # 如果IP存在重复项 这里的set()可以去除重复项。
            dic_ip_now, list_items_duplicate, str_duplicate_items = self.remove_duplicates(dic_ip_mps, list_items)
        else:
            dic_ip_now = dic_ip_mps
                
        # 返回去重后的项目id、重复项目的名称
        print("当前满足条件的所有项(andon_mps.id:ip):%s\n对应andon_menu表中的项(andon_menu.id:ip):%s" % (dic_ip_mps, dic_ip_menu))
        print("去重后满足条件的项目:%s, 重复的项目列表:%s, 重复的项目字符串:%s" % (dic_ip_now, list_items_duplicate, str_duplicate_items))
        print("所有项目:", list_items)
        return dic_ip_now, str_duplicate_items

    # 去除重复项
    def remove_duplicates(self, dic_ip_mps={}, list_items=[]):
        print("进入remove_duplicates")
        logging.info('进入remove_duplicates')
        list_duplicate_ip = [x for x in Counter(list(dic_ip_mps.values())) if list(dic_ip_mps.values()).count(x) > 1]
        list_unique_ip = [x for x in Counter(list(dic_ip_mps.values())) if list(dic_ip_mps.values()).count(x) == 1]
        list_items_duplicate = [(x[0], x[1]) for x in list_items if x[1] in list_duplicate_ip]
        str_duplicate_items = "发现重复项:\n"
        for item in list_items_duplicate:
            str_duplicate_items += "项目名称:%s, PLC的ip:%s \n" % item
        dic_ip_now = {key:value for key, value in dic_ip_mps.items() if value in list_unique_ip}  # 不重复的项
        return dic_ip_now, list_items_duplicate, str_duplicate_items

    # 项目归类（1、本次采集的所有项目的id；2、需要删除的项目的id；3、采集后留下的项目id--存放在list_id_old中，用于下次判断使用)
    def item_classify(self, dic_ip_old={}, dic_ip_now={}):
        print("进入id_classify")
        logging.info('进入id_classify')
        dic_plc_reset = {}  # 存储需要清零重置的项目
        dic_ip_last = {}    # 存储需要最后采集一次的项目
        
        # 获取需要删除(清零的项目)的项目,key:value对应andon_mps.id:andon_menu.ip--首次采集需清零。在dic_ip_new中但不在dic_ip_old中的项目即为首次采集的项目
        dic_plc_reset = {key:value for key, value in dic_ip_now.items() if key not in dic_ip_old.keys()}
        
        # 获取需要最后采集一次的项目,key:value对应andon_mps.id:andon_menu.ip--进行末次采集。在dic_ip_old中但不在dic_ip_new中的项目即为末次采集的项目
        dic_ip_last = {key:value for key, value in dic_ip_old.items() if key not in dic_ip_now.keys()}
         
        if len(dic_plc_reset) > 0:  # 如果存在需要清零的项目  
            # 如果dic_plc_reset中对应的项目在andon_history表中已存在数据，则表明该项目对应的PLC不应被清零（可能是由于程序重启等因素导致的dic_ip_old被清空）
            # 因此该项目应从dic_plc_reset中剔除
            # 1、创建搜索条件
            s = ''
            keys = list(dic_plc_reset.keys())
            l = len(keys)
            for i in range(l):
                if i < l - 1:  # 如果不是最后一位，在后面添加or关键字
                    s += "mps_info_id=%s or " % keys[i]
                else:  # 如果是最后一位
                    s += "mps_info_id=%s" % keys[i]
            # 2、创建SQL语句:查询dic_plc_reset项目在andon_history中的所有记录
            sql = "SELECT DISTINCT mps_info_id FROM andon_history WHERE " + s
            print(sql)
            # 3、执行查询
            rows = self.select_query(sql)
            print(rows, dic_plc_reset)
            if len(rows) > 0:  # 若存在数据
                for row in rows:
                    dic_plc_reset.pop(row[0])
            else:
                pass
        else:
            pass
        
        # 采集后dic_ip_new存放在dic_ip_old中，用于下次判断使用
        dic_ip_old = dic_ip_now
        return dic_ip_old, dic_plc_reset, dic_ip_last

    # 采集PLC数据
    def data_collection(self, dic_id_ip={}):
        print("进入last_data_collection")
        logging.info('进入last_data_collection')
        values = []  # 存储连接失败的PLC的ip
        dic_to_database = {}  # 存储需要采集的项目的数据，准备放入数据库。格式--key：value = andon_mps.id:plc的数据
        for key, value in dic_id_ip.items():
            siemens = SiemensS7Net(SiemensPLCS.S200Smart, value)
            # 建立PLC长连接
            if siemens.ConnectServer().IsSuccess:  # 连接成功，读取PLC的当前计数VW10, 并存入dic_to_database
                dic_to_database[key] = siemens.ReadInt16("DB1.10").Content
                siemens.ConnectClose()  # 关闭PLC连接
            else:  # 若连接失败，则发送邮件至相关人员
                values.append(value)

        if len(values) > 0:
            self.plc_connect_failed(values)
        return dic_to_database
        
    # PLC清零
    def plc_reset(self, dic_plc_reset={}):
        print("进入plc_reset")
        logging.info('进入plc_reset')
        plc_reset_count = len(dic_plc_reset)
        if plc_reset_count > 0:  # 若存在需要清零的PLC
            for key, value in dic_plc_reset.items():
                siemens = SiemensS7Net(SiemensPLCS.S200Smart, value)  # 创建PLC实例
                if siemens.ConnectServer().IsSuccess:  # 若连接成功
                    while not siemens.ReadBool("M0.0").Content:  # 给M0.0赋值为True，直到成功
                        siemens.WriteBool("M0.0", True)  # 通知PLC清零
                        sleep(2)
                    while siemens.ReadBool("M0.0").Content:  # 如果置零信号成功发送，给M0.0赋值为False，直到成功
                        siemens.WriteBool("M0.0", False)
                        sleep(2)
                    siemens.ConnectClose()  # 关闭PLC连接
                else:  # 建立PLC长连接失败，发送邮件至相关人员
                    self.plc_connect_failed(value)
        else:
            pass
        
    # 将采集的数据存储到数据库中--数据包括末次采集项目，以及当前项目
    def data_save(self, data_to_database_last={}, data_to_database_now={}):
        print('进入data_save')
        logging.info('进入data_save')
        # 合并，得到所有需要保存的数据
        data_to_database = {** data_to_database_last, **data_to_database_now}
        if len(data_to_database) > 0:  # 若存在需要存储的数据
            sql = 'INSERT INTO andon_history (mps_info_id, actual_outputs, input_datetime) VALUES '
            for key, value in data_to_database.items():
                sql += "(%s, %s, now()), " % (key, value)  # 这里的", "不能去掉，因为如果有多个的数据，则需要分割开来.now()表示使用系统当前时间
            sql = sql[:-2]  # 去掉sql的末尾的两位(一个","和一个" ")
            print(sql)
            self.insert_query(sql)

    # 异常信息发送邮件通知
    def send_email(self, message=''):
        print('进入send_email')
        logging.info('进入send_email')
        sender = 'yulin.wei@huf-group.com'          # 发送邮件者
        sql = 'SELECT * FROM public.andon_managers'
        rows = self.select_query(sql)
        receivers = [row[2] for row in rows]  # 接收邮件者
        mail_msg = message
        message = MIMEText(mail_msg, 'plain', 'utf-8')
        message['From'] = sender
        message['To'] = ";".join(receivers)
 
        subject = 'Andon系统 自动 邮件测试'
        message['Subject'] = Header(subject, 'utf-8')
        try:
            smtpObj = smtplib.SMTP('10.40.3.108')
            smtpObj.sendmail(sender, receivers, message.as_string())
            print("邮件发送成功")
            logging.info('邮件发送成功')
        except smtplib.SMTPException:
            print("Error: 无法发送邮件")
            logging.error('无法发送邮件')
