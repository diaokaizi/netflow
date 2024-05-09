import pymysql

class MySQL:
    def __init__(self):
        self.connc = pymysql.Connect(
                    host='127.0.0.1',
                    user='root',
                    password="123456",
                    database='web',
                    port=3306,
                    charset="utf8",
        )
    
    def execute(self, sql:str):
        cursor = self.connc.cursor()
        try:
            cursor.execute(sql)
            self.connc.commit()
            data = cursor.fetchall()
        except Exception as e:
            self.connc.rollback()
            print(e)
        return data

def creat_table():
    sql = """CREATE TABLE IF NOT EXISTS netflow (
        id int NOT NULL AUTO_INCREMENT,
        ip char(20) NOT NULL,
        port int NOT NULL,
        type varchar(30) NOT NULL,
        info varchar(30) NOT NULL,
        create_time timestamp NOT NULL,
        update_time timestamp NOT NULL,
        primary key (id),
        INDEX  time_index (ip, port)
        )ENGINE=InnoDB DEFAULT CHARSET=utf8"""
    MySQL().execute(sql=sql)