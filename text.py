# from clickhouse_test import get_clickhouse_client
# from sqlalchemy import create_engine, Column, MetaData

# from clickhouse_sqlalchemy import (
#     Table, make_session, get_declarative_base, types, engines
# )
import datetime
from clickhouse_sqlalchemy import (
    Table, make_session, get_declarative_base, types, engines
)
from sqlalchemy import create_engine, Column, MetaData
from sqlalchemy import func
from models import NetflowLocal, Netflow
from sqlalchemy import Column, DateTime, String
uri = 'clickhouse+native://localhost/default'
engine = create_engine(uri)
session = make_session(engine)
# res = session.query(NetflowLocal) \
#     .filter(NetflowLocal.time_start > datetime.datetime.now() - datetime.timedelta(minutes=10))
# print(res)
# res = session.query(Netflow.time_start) \
#     .filter(Netflow.time_start > datetime.datetime.strptime('2024-04-11 00:00:00', '%Y-%m-%d %H:%M:%S')) \
#     .filter(Netflow.time_start < datetime.datetime.strptime('2024-04-11 00:20:00', '%Y-%m-%d %H:%M:%S')) \
#     .limit(10) \
#     .all()
# print(res)

# res = session.query(func.sum(Netflow.bytes)) \
res = session.query(func.count(NetflowLocal.bytes)) \
    .filter(NetflowLocal.srcIP == "193.62.193.165") \
    .all()
print(res)


    # .filter(NetflowLocal.time_start > datetime.datetime.strptime('2024-03-11 00:00:00', '%Y-%m-%d %H:%M:%S')) \
    # .filter(NetflowLocal.time_start < datetime.datetime.strptime('2024-03-12 00:30:00', '%Y-%m-%d %H:%M:%S')) \
# engine = create_engine(uri)
# session = make_session(engine)
# metadata = MetaData(bind=engine)
# Base = get_declarative_base(metadata=metadata)
# class NetflowLocal(Base):
#     time_start = Column(types.DateTime, primary_key=True)
#     srcIP = Column(types.String)
#     dstIP = Column(types.String)
#     srcTransportPort = Column(types.Int16)
#     dstTransportPort = Column(types.Int16)
#     transport = Column(types.String)
#     bgpSrcAsNumber = Column(types.Int16)
#     bgpDstAsNumber = Column(types.Int16)
#     bytes = Column(types.Int32)
#     packets = Column(types.Int16)
#     flowDirection = Column(types.UInt8)
#     ingressInterface = Column(types.UInt8)
#     egressInterface = Column(types.UInt8)
#     __table_args__ = (
#         engines.Memory(), 
#     )
# # Emits CREATE TABLE statement
# # NetflowLocal.__table__.create()

# # Successfully installed SQLAlchemy-1.4.52 inflect-7.2.0 sqlacodegen-2.3.0.post1 typeguard-4.2.1