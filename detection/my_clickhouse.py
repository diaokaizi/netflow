import clickhouse_connect
class MyClickhouse():
    def __init__(self, host:str="223.193.36.224", username:str="default"):
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
        return conn.command(sql)

# self.conn = clickhouse_connect.get_client(host=, username='default')\
    
# sql = "select * from netflow_140 limit 1"
sql1 = "select start from netflow_140 where srcIP='159.226.227.107' and srcTransportPort='80' order by start desc limit 1"
sql2 = "select start from netflow_140 where dstIP='159.226.227.107' and dstTransportPort='80' order by start desc limit 1"
print(MyClickhouse().command(sql1))
print(MyClickhouse().command(sql2))
