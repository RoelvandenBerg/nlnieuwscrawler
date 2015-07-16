__author__ = 'roelvdberg@gmail.com'
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from crawler.settings import *

engine = create_engine('sqlite:///' + DATABASE_FILENAME, echo=VERBOSE)
Base = declarative_base()

Session = sessionmaker(bind=engine)

class Paragraph(Base):
    __tablename__ = 'paragraphs'

    id = Column(Integer, primary_key=True)
    site = Column(String)
    crawl_datetime = Column(DateTime)
    datetime = Column(DateTime)
    title = Column(String)
    description = Column(String)
    author = Column(String)
    published_time = Column(DateTime)
    expiration_time = Column(DateTime)
    section = Column(String)
    tag = Column(String)
    paragraph = Column(String, nullable=False)
    url = Column(String)

    def __repr__(self):
       return "<Title(title={}, date crawled={})>".format(
                            self.title, str(self.datetime))


def create_all():
    Base.metadata.create_all(engine)

if __name__ == '__main__':
    create_all()
