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
    datetime = Column(DateTime)
    site = Column(String)
    paragraph = Column(String)
    url = Column(String)

    def __repr__(self):
       return "<Title(site={}, date={})>".format(
                            self.site, str(self.datetime))


if __name__ == '__main__':
    Base.metadata.create_all(engine)