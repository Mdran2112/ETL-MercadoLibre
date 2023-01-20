from sqlalchemy import Column, String, Float, DateTime, Integer

from database import Base


class RequestMetrics(Base):
    __tablename__ = "request_metrics"

    rowid = Column(Integer(), primary_key=True)
    date = Column(DateTime())
    api_name = Column(String(50), nullable=False)
    request_time = Column(Float(), nullable=False)


class DatabaseMetrics(Base):
    __tablename__ = "database_metrics"

    rowid = Column(Integer(), primary_key=True)
    date = Column(DateTime())
    table_name = Column(String(50), nullable=False)
    insert_time = Column(Float(), nullable=False)


class ProcessMetrics(Base):
    __tablename__ = "process_metrics"

    rowid = Column(Integer(), primary_key=True)
    date = Column(DateTime())
    process_name = Column(String(50), nullable=False)
    process_time = Column(Float(), nullable=False)
