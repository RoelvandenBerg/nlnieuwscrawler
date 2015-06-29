__author__ = 'roelvdberg@gmail.com'

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///' + DATABASE_FILENAME, echo=VERBOSE)
Base = declarative_base()

Session = sessionmaker(bind=engine)

class Site(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime)
    title = Column(String)
    description = Column(String)
    text = Column(String)
    url = Column(String)
    site = Column(String)

    def __repr__(self):
       return "<Title(title={}, date={})>".format(
                            self.title, str(self.datetime))


if __name__ == '__main__':
    Base.metadata.create_all(engine)