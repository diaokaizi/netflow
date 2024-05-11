import clickhouse_connect
import requests
import json
import datetime
from urllib import parse
import ipaddress
import traceback
from random import sample
from IPy import IP, IPSet
from subprocess import getoutput

import pandas as pd
class NmapScan:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.nmap_out_text = None
        self.nmap_out_port = ''
        self.state = ''
        self.service = ''
        self.version = ''
        self.nmap_datetime = None

    def __str__(self) -> str:
        return json.dumps(self.__dict__)
    
    def clickhouse_data(self) -> str:
        return list(self.__dict__.values())
    
    def do_nmap(self):
        self.nmap_datetime = datetime.datetime.now()
        try:
            nmap_out = getoutput(f"nmap -sV {self.ip} -p {self.port} -Pn")
            nmap_out = nmap_out.split(" VERSION\n")[1].split("\n")[0]
            self.nmap_out_text = nmap_out
            self.prase_nmap_out_text()
        except:
            pass
    
    def prase_nmap_out_text(self):
        if self.nmap_out_text is None:
            return
        text = self.nmap_out_text
        self.nmap_out_port = ''
        self.state = ''
        self.service = ''
        self.version = ''
        try:
            self.nmap_out_port = text[0:text.find(' ')]
            text = text[text.find(' '):].strip()

            self.state = text[0:text.find(' ')]
            text = text[text.find(' '):].strip()

            index = text.find(' ')
            if index == -1:
                self.service = text
            else:
                self.service = text[0:index]
                self.version = text[index:].strip()
        except:
            pass

    

def nmap_scan():
    with open("/root/work/jobs/service_discover_res.log", "r") as f:  # 打开文件
        data = f.read().split()
    with open("/root/work/jobs/service_discover_res_result.log","a") as f:
        for socket in data:
            socket = socket.split(',')


    # end_datetime = datetime.datetime.strptime("2024-05-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    # my_clickhouse = MyClickhouse()
    # with open("log.log","a") as f:
    #     while start_datetime < end_datetime:
    #         try:
    #             # netflow服务
    #             print("{start_datetime},{len}\n".format(start_datetime=start_datetime, len=len(data_clickhouse)))
    #         except Exception as ex:
    #             f.write("########## error{start_datetime},{host},{ex}, {detail}\n".format(start_datetime=start_datetime, host=host, ex=ex, detail=traceback.format_exc()))
    #         start_datetime = start_datetime + datetime.timedelta(minutes=5)
    # my_clickhouse.close_conn()

def prase_nmapout_log_05_11():
    with open("/root/work/jobs/service_discover_res.log","r") as f:
        data = f.read().split('\n')
        res = []
    for _ in data:
        try:
            _ = _[_.find("'nmap_ip': '") + len("'nmap_ip': '"):]
            ip = _[0:_.find("'")]
            _ = _[_.find("'"):]
            text = _.split("'nmap_res': '")[1].split("'")[0]
            nmap_out_port = ''
            state = ''
            service = ''
            version = ''
            try:
                nmap_out_port = text[0:text.find(' ')]
                text = text[text.find(' '):].strip()

                state = text[0:text.find(' ')]
                text = text[text.find(' '):].strip()

                index = text.find(' ')
                if index == -1:
                    service = text
                else:
                    service = text[0:index]
                    version = text[index:].strip()
            except:
                pass
            res.append([ip, nmap_out_port, state, service, version])
        except:
            pass
    
    src_data_df = pd.DataFrame(res, columns =['ip', 'nmap_out_port', 'state', 'service', 'version'])
    filepath = r'res.xlsx'
    src_data_df.to_excel(filepath, index=False)

def analyze_nmap_result_05_11():
    def classify(row):
        ser = row['service']
        if 'domain' in ser or 'dns' in ser:
            return 'dns'
        if 'ftp'in ser:
            return 'ftp'
        if 'ssh'in ser:
            return 'ssh'
        elif 'netbus' in ser:
            return 'netbus'
        elif 'telnet' in ser:
            return 'telnet'
        elif 'websocket' in ser:
            return 'websocket'
        elif 'snmp' in ser:
            return 'snmp'
        elif 'pop3' in ser:
            return 'pop3'
        elif 'bittorrent' in ser:
            return 'bittorrent'
        elif 'ipcam' in ser:
            return 'ipcam'
        elif 'ms-wbt-server' in ser:
            return 'rdp'
        elif 'rtsp' in ser:
            return 'rtsp'
        elif 'http' in ser:
            return 'http'
        elif 'unknown' in ser:
            return 'unknown'
        else:
            return 'other'
    def get_port(row):
        ser = row['nmap_out_port']
        return ser.split('/')[0]
    data = pd.read_excel(io=r'res.xlsx')
    data['type'] = data.apply(classify, axis=1)
    data['port'] = data.apply(get_port, axis=1)
    filepath = r'res_.xlsx'
    data.to_excel(filepath, index=False)
    # value_counts.to_excel('res_value_counts.xlsx', index=False)

def analyze_nmap_result_05_11_v2():
    data = pd.read_excel(io=r'res_.xlsx')
    group  = data.groupby('type')
    for key, df in group:
        print(key)
    # value_counts.to_excel('res_value_counts.xlsx', index=False)
        count = df['port'].value_counts().to_frame().reset_index()
        print(count)

if __name__ == '__main__':
    analyze_nmap_result_05_11_v2()
    # get_test_data("223.193.36.79:7140", 5)


    # nmap_scan()
    # res = getoutput(f"nmap -sV 124.16.0.136 -p 80")
    # print(res)

# self.conn = clickhouse_connect.get_client(host=, username='default')\
    
