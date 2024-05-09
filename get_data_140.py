import clickhouse_connect
import requests
import json
import datetime
from urllib import parse
import ipaddress
import traceback
from random import sample
class ClickhouseNetflowObj:
    def __init__(self, start, duration, srcIP, dstIP,
                 srcIPNetwork, dstIPNetwork, srcTransportPort, dstTransportPort,
                 transport, bgpSrcAsNumber, bgpDstAsNumber, bytes, packets, 
                 flowDirection, ingressInterface, egressInterface, nextHopAddress, isIPv6):
        self.start = start
        self.duration = duration
        self.srcIP = srcIP
        self.dstIP = dstIP
        self.srcIPNetwork = srcIPNetwork
        self.dstIPNetwork = dstIPNetwork
        self.srcTransportPort = srcTransportPort
        self.dstTransportPort = dstTransportPort
        self.transport = transport
        self.bgpSrcAsNumber = bgpSrcAsNumber
        self.bgpDstAsNumber = bgpDstAsNumber
        self.bytes = bytes
        self.packets = packets
        self.flowDirection = flowDirection
        self.ingressInterface = ingressInterface
        self.egressInterface = egressInterface
        self.nextHopAddress = nextHopAddress
        self.isIPv6 = isIPv6
    def __str__(self) -> str:
        return json.dumps(self.__dict__)
    
    def clickhouse_data(self) -> str:
        return list(self.__dict__.values())

class MyClickhouse():
    def __init__(self, host:str="localhost", username:str="default"):
        self.host = host
        self.username = username
        self.conn = None
    def get_conn(self):
        if self.conn is None:
            self.conn = clickhouse_connect.get_client(host=self.host, username=self.username)
        return self.conn
    def close_conn(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
    def insert(self, table, data, column_names):
        conn = self.get_conn()
        conn.insert(table=table, data=data, column_names=column_names)
    def command(self, sql):
        conn = self.get_conn()
        conn.command(sql)

def convert_utc_to_local(utc_string):
    # 将UTC字符串转换为local字符串
    utcTime = datetime.datetime.strptime(utc_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    localtime = utcTime + datetime.timedelta(hours=8)
    return localtime

def get_ip_network(ip, prefix_length):
    ip = ip + '/' + str(prefix_length)
    return str(ipaddress.ip_interface(ip).network)

def parse_clickhouse_data(netflow) -> ClickhouseNetflowObj:
    if 'destination_ipv6_address' in netflow["netflow"]:
        # ipv6
        return ClickhouseNetflowObj(
            start=convert_utc_to_local(netflow['event']['start']),
            duration=int(netflow['event']['duration'] / 1000000000),
            srcIP=netflow['source']['ip'], 
            dstIP=netflow['destination']['ip'], 
            srcIPNetwork=get_ip_network(netflow['source']['ip'], netflow['netflow']['source_ipv6_prefix_length']), 
            dstIPNetwork=get_ip_network(netflow['destination']['ip'], netflow['netflow']['destination_ipv6_prefix_length']), 
            srcTransportPort=netflow['source']['port'],
            dstTransportPort=netflow['destination']['port'],
            transport=netflow['network']['transport'],
            bgpSrcAsNumber=netflow['netflow']['bgp_source_as_number'], 
            bgpDstAsNumber=netflow['netflow']['bgp_destination_as_number'], 
            bytes=netflow['network']['bytes'], 
            packets=netflow['network']['packets'],
            flowDirection=netflow['netflow']['flow_direction'],
            ingressInterface=netflow['netflow']['ingress_interface'],
            egressInterface=netflow['netflow']['egress_interface'],
            nextHopAddress=netflow['netflow']['ip_next_hop_ipv6_address'],
            isIPv6=1,
            )
    else:
        # ipv4
        return ClickhouseNetflowObj(
            start=convert_utc_to_local(netflow['event']['start']),
            duration=int(netflow['event']['duration'] / 1000000000),
            srcIP=netflow['source']['ip'], 
            dstIP=netflow['destination']['ip'], 
            srcIPNetwork=get_ip_network(netflow['source']['ip'], netflow['srcPrefixLength']), 
            dstIPNetwork=get_ip_network(netflow['destination']['ip'], netflow['dstPrefixLength']), 
            srcTransportPort=netflow['source']['port'],
            dstTransportPort=netflow['destination']['port'],
            transport=netflow['network']['transport'],
            bgpSrcAsNumber=netflow['bgpSrcAsNumber'], 
            bgpDstAsNumber=netflow['bgpDstAsNumber'], 
            bytes=netflow['network']['bytes'], 
            packets=netflow['network']['packets'],
            flowDirection=netflow['flowDirection'],
            ingressInterface=netflow['ingressInterface'],
            egressInterface=netflow['egressInterface'],
            nextHopAddress=netflow['nextHopAddress'],
            isIPv6=0,
            )

def creat_netflow_table(host):
    myclickhouse = MyClickhouse()
    myclickhouse.command(
        f"CREATE TABLE IF NOT EXISTS {get_table_name(host)} (\
            start DateTime('Asia/Shanghai'), \
            duration UInt16, \
            srcIP String, \
            dstIP String, \
            srcIPNetwork String, \
            dstIPNetwork String, \
            srcTransportPort UInt16, \
            dstTransportPort UInt16, \
            transport String, \
            bgpSrcAsNumber UInt16, \
            bgpDstAsNumber UInt16, \
            bytes UInt32, \
            packets UInt16, \
            flowDirection UInt8, \
            ingressInterface UInt8, \
            egressInterface UInt8, \
            nextHopAddress String, \
            isIPv6 UInt8, \
            )\
            ENGINE MergeTree \
            ORDER BY start \
            TTL start + INTERVAL 1 MONTH\
            SETTINGS storage_policy = 'local' \
            ")
    myclickhouse.close_conn()

def get_table_name(host:str):
    return f"netflow_{host[-3:]}"

def lokiapi(host: str, start: datetime.datetime, end: datetime.datetime, limit: int = None) -> list[ClickhouseNetflowObj]:
    query_data = {
        'start': start.timestamp(),
        'end': end.timestamp()
    }
    if limit is not None:
        query_data['limit'] = limit
    query = parse.urlencode(query=query_data)
    base_url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}"
    loki_url = f'{base_url}&{query}'
    data_clickhouse = []
    try:
        resp = requests.get(loki_url)
        # url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}&limit={limit}&start={startdt.timestamp()}&end={enddt.timestamp()}"
        data_clickhouse = parse_lokiapi_data(resp.text)
    except:
        print(start, loki_url, traceback.format_exc())
    return data_clickhouse
    
def parse_lokiapi_data(data) -> list[ClickhouseNetflowObj]:
    source = json.loads(data)['data']['result'][0]['values']
    newflows = [json.loads(record[1]) for record in source]

    data_clickhouse = []
    for newflow in newflows:
        try:
            data_clickhouse.append(parse_clickhouse_data(newflow))
        except Exception as ex:
            pass
    return data_clickhouse

def get_data_by_minutes(host, start, minutes) -> list[ClickhouseNetflowObj]:
    end = start + datetime.timedelta(minutes=minutes)
    return lokiapi(host=host, start=start, end=end, limit=500000)

def netflow_service(host:str, start_datetime:datetime.datetime, minutes:int, log_path:str, my_clickhouse:MyClickhouse) -> list[ClickhouseNetflowObj]:
    
    # 获取数据
    data_clickhouse = get_data_by_minutes(host=host, start=start_datetime, minutes=minutes)

    # 插入click house的netflow数据
    data = [obj.clickhouse_data() for obj in data_clickhouse]
    data = [t for t in data if None not in t]
    my_clickhouse.insert(table=get_table_name(host=host), data=data, column_names=[
        'start', 'duration', 'srcIP', 'dstIP', 'srcIPNetwork', 'dstIPNetwork', 
        'srcTransportPort', 'dstTransportPort', 'transport', 'bgpSrcAsNumber', 'bgpDstAsNumber', 'bytes', 'packets', 'flowDirection',
        'ingressInterface', 'egressInterface', 'nextHopAddress', 'isIPv6'])

    return data_clickhouse



def get_latest_start_datetime(minutes):
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    second = minutes * 60
    return datetime.datetime.fromtimestamp(timestamp - timestamp % second - second)

def get_data_140_job(host:str, minutes:int):
    start_datetime = datetime.datetime.strptime("2024-04-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.datetime.strptime("2024-04-01 03:00:00", "%Y-%m-%d %H:%M:%S")
    my_clickhouse = MyClickhouse()
    with open("log.log","a") as f:
        while start_datetime < end_datetime:
            try:
                start_datetime = get_latest_start_datetime(minutes=minutes)
                
                # netflow服务
                data_clickhouse = netflow_service(host=host, start_datetime=start_datetime, minutes=minutes, log_path="path", my_clickhouse=my_clickhouse)
                f.write("{start_datetime},{len}\n".format(start_datetime=start_datetime, len=len(data_clickhouse)))
            except Exception as ex:
                f.write("########## error{start_datetime},{host},{ex}, {detail}\n".format(start_datetime=start_datetime, host=host, ex=ex, detail=traceback.format_exc()))
            start_datetime = start_datetime + datetime.timedelta(minutes=5)
    my_clickhouse.close_conn()
def creat_netflow_table(host):
    myclickhouse = MyClickhouse()
    myclickhouse.command(
        f"CREATE TABLE IF NOT EXISTS {get_table_name(host)} (\
            start DateTime('Asia/Shanghai'), \
            duration UInt16, \
            srcIP String, \
            dstIP String, \
            srcIPNetwork String, \
            dstIPNetwork String, \
            srcTransportPort UInt16, \
            dstTransportPort UInt16, \
            transport String, \
            bgpSrcAsNumber UInt16, \
            bgpDstAsNumber UInt16, \
            bytes UInt32, \
            packets UInt16, \
            flowDirection UInt8, \
            ingressInterface UInt8, \
            egressInterface UInt8, \
            nextHopAddress String, \
            isIPv6 UInt8, \
            )\
            ENGINE MergeTree \
            ORDER BY start \
            TTL start + INTERVAL 1 MONTH\
            ")
    myclickhouse.close_conn()
if __name__ == '__main__':
    creat_netflow_table("223.193.36.79:7140")
    get_data_140_job("223.193.36.79:7140", 5)

# self.conn = clickhouse_connect.get_client(host=, username='default')\
    
