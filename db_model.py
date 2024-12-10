from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, INT

Base = declarative_base()


class Disaster(Base):
    __tablename__ = 'disasters'

    id = Column(INT, primary_key=True, autoincrement=True)
    aircraft = Column(String, nullable=False)
    registration_number = Column(String, nullable=False)
    country = Column(String, nullable=False)
    location = Column(String, nullable=False)
    link = Column(String, nullable=False)