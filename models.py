# coding: utf-8
from sqlalchemy import Column, DateTime, String
from clickhouse_sqlalchemy.types.common import UInt16, UInt32, UInt8
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Netflow(Base):
    __tablename__ = 'netflow'

    time_start = Column(DateTime("'Asia/Shanghai'"), primary_key=True)
    srcIP = Column(String, nullable=False)
    dstIP = Column(String, nullable=False)
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


class NetflowLocal(Base):
    __tablename__ = 'netflow_local'

    time_start = Column(DateTime("'Asia/Shanghai'"), primary_key=True)
    srcIP = Column(String, nullable=False)
    dstIP = Column(String, nullable=False)
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
