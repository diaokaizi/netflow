import clickhouse_connect
import json
import requests
import json
import datetime
from urllib import parse
import schedule
import time
import threading

def get_clickhouse_client():
    return clickhouse_connect.get_client(host='localhost', username='default', send_receive_timeout=60)

class ClickhouseNetflowObj:
    def __init__(self, time_start, srcIP, dstIP, srcTransportPort, dstTransportPort, 
                 transport, bgpSrcAsNumber, bgpDstAsNumber, bytes, packets, 
                 flowDirection, ingressInterface, egressInterface):
        self.time_start = time_start
        self.srcIP = srcIP
        self.dstIP = dstIP
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
    def __str__(self) -> str:
        return json.dumps(self.__dict__)
    
    def clickhouse_data(self) -> str:
        return list(self.__dict__.values())

def convert_utc_to_local(utc_string):
    # 将UTC字符串转换为local字符串
    utcTime = datetime.datetime.strptime(utc_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    localtime = utcTime + datetime.timedelta(hours=8)
    return localtime

def parse_ipv4_data(newflow) -> ClickhouseNetflowObj:
    return ClickhouseNetflowObj(
        time_start=convert_utc_to_local(newflow['event']['start']),
        srcIP=newflow['source']['ip'], 
        dstIP=newflow['destination']['ip'], 
        srcTransportPort=newflow['source']['port'],
        dstTransportPort=newflow['destination']['port'],
        transport=newflow['network']['transport'],
        bgpSrcAsNumber=newflow['bgpSrcAsNumber'], 
        bgpDstAsNumber=newflow['bgpDstAsNumber'], 
        bytes=newflow['network']['bytes'], 
        packets=newflow['network']['packets'],
        flowDirection=newflow['flowDirection'],
        ingressInterface=newflow['ingressInterface'],
        egressInterface=newflow['egressInterface']
        )

# def parse_ipv6_data(newflow) -> NetflowObj:
#     return NetflowObj(
#         start=convert_utc_to_local(newflow['event']['start']),
#         end=convert_utc_to_local(newflow['event']['end']),
#         srcIP=newflow['source']['ip'], 
#         dstIP=newflow['destination']['ip'], 
#         srcTransportPort=newflow['source']['port'],
#         dstTransportPort=newflow['destination']['port'],
#         transport=newflow['network']['transport'],
#         bgpSrcAsNumber=newflow['netflow']['bgp_source_as_number'], 
#         bgpDstAsNumber=newflow['netflow']['bgp_destination_as_number'], 
#         bytes=newflow['network']['bytes'], 
#         packets=newflow['network']['packets'],
#         flowDirection=newflow['netflow']['flow_direction']
#         )

def lokiapi(host: str, start: str, end: str, limit: int = None) -> list[ClickhouseNetflowObj]:
    startdt = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    enddt = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    query_data = {
        'start': startdt.timestamp(),
        'end': enddt.timestamp()
    }
    if limit is not None:
        query_data['limit'] = limit
    query = parse.urlencode(query=query_data)
    base_url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}"
    loki_url = f'{base_url}&{query}'
    try:
        resp = requests.get(loki_url)
        # url = f"http://{host}/loki/api/v1/query_range?query={{job=%22netflow%22}}&limit={limit}&start={startdt.timestamp()}&end={enddt.timestamp()}"
    except:
        raise Exception(f"lokiapi request error {loki_url}")
    try:
        data = parse_lokiapi_clickhouse(resp.text)
    except:
        raise Exception(f"parse_lokiapi_clickhouse error {loki_url}")
    return data

def parse_lokiapi_clickhouse(data) -> list[ClickhouseNetflowObj]:
    try:
        source = json.loads(data)['data']['result'][0]['values']
        newflows = [json.loads(record[1]) for record in source]
        data = []
        for newflow in newflows:
            try:
                #ipv4
                data.append(parse_ipv4_data(newflow))
            except:
                pass
        return data
    except:
        return []

def get_data_by_5m(host, start) -> list[ClickhouseNetflowObj]:
    end = (datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    return lokiapi(host=host, start=start, end=end, limit=1000000)

def get_latest_5m_start_datetime():
    now = datetime.datetime.now()
    timestamp = datetime.datetime.timestamp(now)
    return datetime.datetime.fromtimestamp(timestamp - timestamp % 300 - 300)

def insert_data_clickhouse(start_datetime:datetime.datetime, host:str):
    netflowObjs = get_data_by_5m(host=host, start=start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    data = [obj.clickhouse_data() for obj in netflowObjs]
    get_clickhouse_client().insert('netflow', data, column_names=['time_start', 'srcIP', 'dstIP', 'srcTransportPort', 'dstTransportPort', 
                                                 'transport', 'bgpSrcAsNumber', 'bgpDstAsNumber', 'bytes', 'packets', 'flowDirection',
                                                 'ingressInterface', 'egressInterface'])
    datetime.timedelta(minutes=5)
def creat_table():
    get_clickhouse_client().command(
        "CREATE TABLE IF NOT EXISTS netflow (\
            time_start DateTime('Asia/Shanghai'), \
            srcIP String, \
            dstIP String, \
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
            ) ENGINE MergeTree ORDER BY time_start \
            SETTINGS storage_policy = 'local_s3' \
            ")


if __name__ == '__main__':
    host = "223.193.36.79:7140"
    creat_table()
    start = datetime.datetime.strptime('2024-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.strptime('2024-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    minutes = 5
    creat_table()
    while start < end:
        insert_data_clickhouse(host="223.193.36.79:7140", start_datetime=start)
        start = start + datetime.timedelta(minutes=minutes)
        time.sleep(2)
# /var/log/clickhouse-server/clickhouse-server.err.log