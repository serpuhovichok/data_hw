from sqlalchemy import create_engine
from db_model import Disaster
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import List, Dict
from sqlalchemy.exc import SQLAlchemyError


class Database:
    def __init__(self):
        self.engine = create_engine(f'sqlite:///air_disasters.db')
        Disaster.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self) -> Session:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def save_disasters(self, disasters: List[Dict]):
        with self.get_session() as session:
            for disaster in disasters:
                try:
                    disaster_obj = Disaster(
                        aircraft=disaster['aircraft'],
                        registration_number=disaster['registration_number'],
                        country=disaster['country'],
                        location=disaster['location'],
                        link=disaster['link'],
                    )
                    session.merge(disaster_obj)

                except SQLAlchemyError as e:
                    raise