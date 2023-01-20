from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime

from database import Base


class Item(Base):
    __tablename__ = "items"

    id = Column(String(50), primary_key=True)
    seller_id = Column(Integer())
    title = Column(String(150), nullable=False)
    sold_quantity = Column(Integer(), nullable=False)
    price = Column(Float(), nullable=False)
    warranty = Column(String(50), nullable=True)
    created_at = Column(DateTime(), default=datetime.now())


class ItemShipping(Base):
    __tablename__ = "item_shipping"

    rowid = Column(Integer(), primary_key=True)
    item_id = Column(String(50), nullable=False)
    shipping_method = Column(String(50), nullable=True)
    date = Column(DateTime(), default=datetime.now())
