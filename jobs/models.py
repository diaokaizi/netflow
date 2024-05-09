# coding: utf-8
# sqlacodegen clickhouse+native://localhost/default?charset=utf8 > models.py
from sqlalchemy import Column, DateTime, String
from clickhouse_sqlalchemy.types.common import UInt16, UInt32, UInt8
from sqlalchemy.ext.declarative import declarative_base
import json
Base = declarative_base()
metadata = Base.metadata


class Netflow140(Base):
    __tablename__ = 'netflow_140'

    start = Column(DateTime("'Asia/Shanghai'"), primary_key=True)
    duration = Column(UInt16, nullable=False)
    srcIP = Column(String, nullable=False)
    dstIP = Column(String, nullable=False)
    srcIPNetwork = Column(String, nullable=False)
    dstIPNetwork = Column(String, nullable=False)
    srcTransportPort = Column(UInt16, nullable=False)
    dstTransportPort = Column(UInt16, nullable=False)
    transport = Column(String, nullable=False)
    bgpSrcAsNumber = Column(UInt16, nullable=False)
    bgpDstAsNumber = Column(UInt16, nullable=False)
    bytes = Column(UInt32, nullable=False)
    packets = Column(UInt16, nullable=False)
    flowDirection = Column(UInt8, nullable=False)
    ingressInterface = Column(UInt8, nullable=False)
    egressInterface = Column(UInt8, nullable=False)
    nextHopAddress = Column(String, nullable=False)
    isIPv6 = Column(UInt8, nullable=False)

    def __str__(self) -> str:
        return json.dumps(str(self.__dict__))