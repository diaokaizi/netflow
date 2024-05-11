import clickhouse_connect
import json
import datetime

# import datetime
# from clickhouse_sqlalchemy import (
#     Table, make_session, get_declarative_base, types, engines
# )
# from sqlalchemy import create_engine, Column, MetaData
# from sqlalchemy import func
# from models import Netflow140
# from sqlalchemy import Column, DateTime, String
# uri = 'clickhouse+native://localhost/default'
# engine = create_engine(uri)
# session = make_session(engine)
# # res = session.query(Netflow140) \
# #     .filter(Netflow140.start > datetime.datetime.strptime('2024-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')) \
# #     .filter(Netflow140.start < datetime.datetime.strptime('2024-04-01 00:01:00', '%Y-%m-%d %H:%M:%S')) \
# #     .all()
# ress = session.query(Netflow140.start, Netflow140.duration, Netflow140.srcIP, Netflow140.dstIP, Netflow140.srcIPNetwork, 
#                      Netflow140.dstIPNetwork, Netflow140.srcTransportPort, Netflow140.dstTransportPort, Netflow140.transport, 
#                      Netflow140.bgpSrcAsNumber, Netflow140.bgpDstAsNumber, Netflow140.bytes, Netflow140.packets, Netflow140.flowDirection, 
#                      Netflow140.ingressInterface, Netflow140.egressInterface, Netflow140.nextHopAddress, Netflow140.isIPv6) \
#     .filter(Netflow140.start > datetime.datetime.strptime('2024-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')) \
#     .filter(Netflow140.start < datetime.datetime.strptime('2024-04-01 00:01:00', '%Y-%m-%d %H:%M:%S')) \
#     .all()

# for res in ress:
#     print(res)

# res = session.query(Netflow140).filter(Netflow140.srcIP == "193.62.193.165").all()
# print(session.query(Netflow140).filter(Netflow140.srcIP == "193.62.193.165").statement)
# for a in res[:10]:
#     print(a)
# print(len(res))


# class ClickhouseNetflowObj:
#     def __init__(self, start:datetime.datetime, duration:int, srcIP:str, dstIP:str,
#                  srcIPNetwork:str, dstIPNetwork:str, srcTransportPort:int, dstTransportPort:int,
#                  transport:str, bgpSrcAsNumber:int, bgpDstAsNumber:int, bytes:int, packets:int, 
#                  flowDirection:int, ingressInterface:int, egressInterface:int, nextHopAddress:str, isIPv6:int):
#         self.start = start
#         self.duration = duration
#         self.srcIP = srcIP
#         self.dstIP = dstIP
#         self.srcIPNetwork = srcIPNetwork
#         self.dstIPNetwork = dstIPNetwork
#         self.srcTransportPort = srcTransportPort
#         self.dstTransportPort = dstTransportPort
#         self.transport = transport
#         self.bgpSrcAsNumber = bgpSrcAsNumber
#         self.bgpDstAsNumber = bgpDstAsNumber
#         self.bytes = bytes
#         self.packets = packets
#         self.flowDirection = flowDirection
#         self.ingressInterface = ingressInterface
#         self.egressInterface = egressInterface
#         self.nextHopAddress = nextHopAddress
#         self.isIPv6 = isIPv6
#     def __str__(self) -> str:
#         return json.dumps(str(self.__dict__))
    
#     def clickhouse_data(self) -> str:
#         return list(self.__dict__.values())
    
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
    def query(self, sql):
        conn = self.get_conn()
        return conn.query(sql).result_rows


