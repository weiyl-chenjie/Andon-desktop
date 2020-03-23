# *************系统自带的库*****************
from time import sleep
import logging

# **************自己的文件***************************
from DataCollection import DataCollection

def main():
    LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(pathname)s %(message)s "  # 配置输出日志格式
    DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '  # 配置输出时间的格式，注意月份和天数不要搞乱了
    logging.basicConfig(level=logging.DEBUG,
                        format=LOG_FORMAT,
                        datefmt=DATE_FORMAT,
                        filename=r"d:\andon\logfile.log"  # 有了filename参数就不会直接输出显示到控制台，而是直接写入文件
                        )

    dic_ip_old = {}             # 存储上次的采集项
    dic_ip_now = {}             # 存储本次采集的项（不包括list_delete里的项目)
    dic_ip_last = {}            # 存储需要最后采集一次的项目（即存在于dic_ip_old但不属于dic_ip_now的项目）
    dic_plc_reset = {}          # 存储需要清零重置的项目（即需要清零的项目）
    data_to_database_last = {}  # 存储采集到的需要最后采集一次的项目的数据
    data_to_database_now = {}   # 存储采集到的本次采集的项的数据
    str_duplicate_items = ''    # 存储重复的项目
    stop = False                # while循环是否终止
    
    while not stop:
        try:       
            daq = DataCollection()
            dic_ip_now, str_duplicate_items = daq.find_mps()
            if len(str_duplicate_items) > 0:  # 若存在IP重复项, 发送邮件至相关人员
                daq.send_email(str_duplicate_items)
            dic_ip_old, dic_plc_reset, dic_ip_last = daq.item_classify(dic_ip_old, dic_ip_now)
            if len(dic_ip_last) > 0:    # 若存在需要进行最后一次采集的项目
                data_to_database_last = daq.data_collection(dic_ip_last)  # 刚刚到期的项目，进行最后一次采集
            if len(dic_plc_reset) > 0:  # 若存在需要对PLC置零复位的项目
                daq.plc_reset(dic_plc_reset)  
            data_to_database_now = daq.data_collection(dic_ip_now)  # 采集正在生产的项目
            daq.data_save(data_to_database_last, data_to_database_now)  # 存储数据
            print("*" * 16,'本次采集结束',"*" * 16)

            sleep(600)  # 10分钟采集一次
        except KeyboardInterrupt:
            print("按下了CTRL+C，正常关闭程序")
            logging.info('按下了CTRL+C，正常关闭程序')
            stop = True
        except Exception as e:
            print("异常结束", str(e))
            logging.error(str(e))
            # stop = True
            daq.send_email(str(e))


if __name__ == "__main__":
    main()
