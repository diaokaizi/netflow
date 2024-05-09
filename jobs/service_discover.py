## 基于流记录的服务器发现
from my_clickhouse import MyClickhouse
import datetime
from clickhouse_sqlalchemy import (
    make_session, get_declarative_base, types, engines
)
from models import Netflow140
from sqlalchemy import create_engine, desc
import pandas as pd
class FindTwoWayFlow:
    def __init__(self):
        uri = 'clickhouse+native://localhost/default'
        engine = create_engine(uri)
        self.session = make_session(engine)
    def get_last_record(self, start:datetime.datetime, end:datetime.datetime):
        sql = f"""
            SELECT *
            FROM netflow_140
            WHERE start > '{start.strptime('%Y-%m-%d %H:%M:%S')}' and start < '{end.strptime('%Y-%m-%d %H:%M:%S')}' and srcIP='{obj.dstIP}' and srcTransportPort='{obj.dstTransportPort}'
            ORDER BY start DESC
            LIMIT 1
        """
        self.clickhouse.get_one_by_sql(sql=sql)
    
    def test(self):
        #取2024-04-01 12:00:00 到 12:05:00的数据
        # sql = f"""
        #     SELECT *
        #     FROM netflow_140
        #     WHERE start > '2024-04-01 12:00:00' and start < '2024-04-01 12:05:00'
        #     ORDER BY start DESC
        # """
        all = self.session.query(Netflow140.start, Netflow140.srcIP, Netflow140.dstIP, Netflow140.srcTransportPort, Netflow140.dstTransportPort, Netflow140.transport) \
            .filter(Netflow140.start > datetime.datetime.strptime('2024-04-01 12:00:00', '%Y-%m-%d %H:%M:%S')) \
            .filter(Netflow140.start < datetime.datetime.strptime('2024-04-01 12:00:02', '%Y-%m-%d %H:%M:%S')) \
            .filter(Netflow140.bytes >= 60) \
            .filter(Netflow140.isIPv6 == 0) \
            .all()
        data = []
        for record in all:
            two = self.session.query(Netflow140.start, Netflow140.srcIP, Netflow140.dstIP, Netflow140.srcTransportPort, Netflow140.dstTransportPort, Netflow140.transport) \
            .filter(Netflow140.start > datetime.datetime.strptime('2024-04-01 12:00:02', '%Y-%m-%d %H:%M:%S')) \
            .filter(Netflow140.start < datetime.datetime.strptime('2024-04-01 13:00:10', '%Y-%m-%d %H:%M:%S')) \
            .filter(Netflow140.bytes >= 60) \
            .filter(Netflow140.srcIP == record[2]) \
            .filter(Netflow140.dstIP == record[1]) \
            .filter(Netflow140.srcTransportPort == record[4]) \
            .filter(Netflow140.dstTransportPort == record[3]) \
            .filter(Netflow140.transport == record[5]) \
            .first()
            if two is not None:
                data.append(two)
        data_df = pd.DataFrame(data, columns =['start', 'srcIP', 'dstIP', 'srcTransportPort', 'dstTransportPort', 'transport']) 
        print(data_df)
        data_df['start'] = data_df['start'].map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        data_df = data_df.sort_values(by='start')
        data_df.to_excel("a1.xlsx",index=False,header=False)
        # start = datetime.datetime.strptime("2024-04-01 03:00:00", "%Y-%m-%d %H:%M:%S")
        # end = datetime.datetime.strptime("2024-05-01 00:00:00", "%Y-%m-%d %H:%M:%S")

if __name__ == '__main__':
    MyClickhouse().command(
        """
select srcTransportPort, count(*) as c
from netflow_140
where start > '2024-04-01 00:00:00' and start < '2024-04-02 00:00:00'
group by srcTransportPort
order by c desc
limit 100
        """
    )
    FindTwoWayFlow().test()
    # data = [
    #     [1, 2, "123"],
    #     [1, 2, "123"],
    #     [1, 2, "123"]
    # ]
    # data_df = pd.DataFrame(data)
    # print(data_df)
    
# dataframe.to_excel("D:实验数据.xls" , sheet_name="data", na_rep="na_test",header=0)
# data = pd.read_excel(r"D://实验数据.xls", sheet_name="data", sheet_index = 1, header = 0)