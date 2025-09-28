from sqlalchemy import Column, Integer, String
from .db import Base

class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, unique=True, index=True, nullable=False)
    qty = Column(Integer, nullable=False, default=0)
    version = Column(Integer, nullable=False, default=1)
    __mapper_args__ = {"version_id_col": version}