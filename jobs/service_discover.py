## 基于流记录的服务器发现
from my_clickhouse import MyClickhouse
import datetime
from clickhouse_sqlalchemy import (
    make_session, get_declarative_base, types, engines
)
from models import Netflow140
from sqlalchemy import create_engine, desc, and_, or_
import pandas as pd
from IPy import IP, IPSet
from collections import Counter
import math
import json
from my_nmap import NmapScan
class FindTwoWayFlow:
    def __init__(self):
        uri = 'clickhouse+native://localhost/default'
        engine = create_engine(uri)
        self.session = make_session(engine)
    
    def test(self):
        # （IP， port）
        # 1. 有效流记录数 2.对端端口数量 
        """
            1.选取一定的流记录，一条流记录得到ip1, ip2
            2.查询ip1, ip2的历史交互记录，历史交互记录数量较小，则判断该交互是扫描或连接流量等无效流量，跳过
            3.判断双方交互流量的端口使用情况
                1.host1端口重复使用，host2端口频繁变化，host1表现服务器特征行为，host2表现客户端特征行为
                1.host1、2端口重复使用，host1、2表现P2P特征行为
                1.host1、2端口频繁变化，无法判断，可能是经过NAT转换导致端口改变等导致
        """
        ############# 配置
        start_datetime_str = "2024-04-01 00:00:00"
        end_datetime_str = "2024-04-01 01:00:00"
        table_name = "netflow_140"
        # 科技网IP
        CSTNET_IP_ALL = ['36.254.0.0/16', '49.210.0.0/15', '60.245.128.0/17', '101.252.0.0/15', '103.2.208.0/22', 
                '116.90.184.0/21', '116.215.0.0/16', '117.103.16.0/20', '119.78.0.0/15', '119.232.0.0/16', 
        '119.233.0.0/17', '124.16.0.0/15', '150.242.4.0/22', '159.226.0.0/16', '202.38.128.0/23', 
        '202.90.224.0/21', '202.122.32.0/21', '202.127.0.0/21', '202.127.16.0/20', '202.127.144.0/20', 
        '202.127.200.0/21', '210.72.0.0/17', '210.72.128.0/19', '210.73.0.0/18', '210.75.160.0/19', 
        '210.75.224.0/19', '210.76.192.0/19', '210.77.0.0/19', '210.77.64.0/19', '211.147.192.0/20', 
        '211.156.64.0/20', '211.156.160.0/20', '211.167.160.0/20', '218.244.64.0/19', '223.192.0.0/15']




        clickhouse = MyClickhouse(host="223.193.36.157")
        start_datetime = datetime.datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")
        nmap_keys = set()

        local_clickhouse =  MyClickhouse()
        while start_datetime < end_datetime:
            # 查询IP对
            row = clickhouse.query(f"""
                SELECT
                    srcIP,
                    dstIP
                FROM {table_name}
                WHERE (start > '{start_datetime}') AND (start < '{start_datetime + datetime.timedelta(minutes=1)}') AND (isIPv6 = 0)
            """)
            start_datetime = start_datetime + datetime.timedelta(minutes=1)
            p0 = IP(_[0])
            p1 = IP(_[1])
            ip_pair = [p0, p1] if p0 < p1 else [p1, p0]      

            local_clickhouse.query(f"""
                SELECT
                    srcIP,
                    dstIP
                FROM ip_pair
                WHERE (p0 = '{str(p0)}') AND (p1 = '{str(p1)}')')
            """
            )


            cstnet_ip_set = IPSet([IP(t) for t in CSTNET_IP_ALL])
            def is_cstnet_ip(x):
                for cstnet_ip in cstnet_ip_set:
                    if x[0] in cstnet_ip or x[1] in cstnet_ip:
                        return True
                return False
            row2 = list(filter(is_cstnet_ip, row1))

            row3 = []
            # 查询历史
            for _ in row2:
                history_record = clickhouse.query(f"""
                    SELECT
                        srcIP,
                        dstIP,
                        srcTransportPort,
                        dstTransportPort,
                    FROM netflow_140
                    WHERE (start > '2024-04-01 00:00:00') AND (start < '2024-04-01 12:00:02') AND (isIPv6 = 0)
                        AND ((srcIP = '{_[0]}' AND dstIP = '{_[1]}') OR (srcIP = '{_[1]}' AND dstIP = '{_[0]}'))
                """)
                if len(history_record) < 5:
                    continue
                p0_port_list = []
                p1_port_list = []
                ip0_is_src_count = 0
                for i in history_record:
                    p0 = IP(i[0])
                    p1 = IP(i[1])
                    if p0 == _[0] and p1 == _[1]:
                        ip0_is_src_count = ip0_is_src_count + 1
                        p0_port_list.append(i[2])
                        p1_port_list.append(i[3])
                    elif p0 == _[1] and p1 == _[0]:
                        p0_port_list.append(i[3])
                        p1_port_list.append(i[2])


                ip0_port_counter = Counter(p0_port_list)
                ip1_port_counter = Counter(p1_port_list)
                # prob = {i[0]:i[1]/len(p0_port_list) for i in ip0_port_counter.items()}      # 计算每个变量的 p*log(p)
                ip0_port_h = - sum([i[1]*math.log2(i[1]) for i in {i[0]:i[1]/len(p0_port_list) for i in ip0_port_counter.items()}.items()]) # 计算熵
                ip1_port_h = - sum([i[1]*math.log2(i[1]) for i in {i[0]:i[1]/len(p1_port_list) for i in ip1_port_counter.items()}.items()]) # 计算熵
                dict = {
                    "ip0" : _[0],
                    "ip0_most_port" : Counter(p0_port_list).most_common(3),
                    "ip0_port_h" : ip0_port_h,
                    "ip1" : _[1],
                    "ip1_most_port" : Counter(p1_port_list).most_common(3),
                    "ip1_port_h" : ip1_port_h,
                    "total_record" : len(history_record),
                    "ip0_is_src_rate" : ip0_is_src_count / len(history_record)
                }


                if ip0_port_h <= ip1_port_h:
                    nmap = NmapScan(ip=_[0], port=Counter(p0_port_list).most_common(1)[0][0])
                else:
                    nmap = NmapScan(ip=_[1], port=Counter(p1_port_list).most_common(1)[0][0])
                key = f"{nmap.ip}:{nmap.port}"
                if key in nmap_keys:
                    continue
                nmap_keys.add(key)
                nmap.do_nmap()
                dict["nmap_ip"] = key
                dict["nmap_res"] = nmap.nmap_out_text
                dict["nmap_time"] = datetime.datetime.now()
                with open("/root/work/jobs/service_discover_res.log","a") as f:
                    f.write(str(dict) + '\n')

        # start = datetime.datetime.strptime("2024-04-01 03:00:00", "%Y-%m-%d %H:%M:%S")
        # end = datetime.datetime.strptime("2024-05-01 00:00:00", "%Y-%m-%d %H:%M:%S")

if __name__ == '__main__':
    local_clickhouse = MyClickhouse()
    # FindTwoWayFlow().test()
    utcTime = datetime.datetime.now()
    localtime = utcTime + datetime.timedelta(hours=8)
    local_clickhouse.insert(table="ip_pair", data=[(
        localtime, "1.1.1.1", 1, 2, 0.1, 3, "1.1.1.2", 1, 2, 0.1, 3
     )], column_names=[
    'start',
    'ip0', 'ip0_most_port', 'ip0_port_count', 'ip0_port_entropy', 'ip0_is_src_count',
    'ip1', 'ip1_most_port', 'ip1_port_count', 'ip1_port_entropy', 'ip1_is_src_count',])
    p0 = IP("1.1.1.1")
    p1 = IP("1.1.1.2")
    r = local_clickhouse.query(f"""
            SELECT
                count(*)
            FROM ip_pair
            WHERE (ip0 = '{str(p0)}') AND (ip1 = '{str(p1)}')
        """
        )
    print(r)


#     MyClickhouse().command(
#         """
# select srcTransportPort, count(*) as c
# from netflow_140
# where start > '2024-04-01 00:00:00' and start < '2024-04-02 00:00:00'
# group by srcTransportPort
# order by c desc
# limit 100
#         """
#     )
    # data = [
    #     [1, 2, "123"],
    #     [1, 2, "123"],
    #     [1, 2, "123"]
    # ]
    # data_df = pd.DataFrame(data)
    # print(data_df)
    
# dataframe.to_excel("D:实验数据.xls" , sheet_name="data", na_rep="na_test",header=0)
# data = pd.read_excel(r"D://实验数据.xls", sheet_name="data", sheet_index = 1, header = 0)