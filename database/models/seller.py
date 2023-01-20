from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime

from database import Base


class Seller(Base):
    __tablename__ = "sellers"

    rowid = Column(Integer(), primary_key=True)
    seller_id = Column(Integer())
    completed_sales = Column(Integer(), nullable=False)
    date = Column(DateTime(), default=datetime.now())
