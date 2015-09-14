__author__ = 'roelvdberg@gmail.com'
import os

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker

from crawler.settings import *

engine = create_engine('sqlite:///' + DATABASE_FILENAME, echo=VERBOSE)

Base = declarative_base()

Session = sessionmaker(bind=engine)


class Website(Base):
    __tablename__ = 'websites'
    id = Column(Integer, primary_key=True)
    webpages = relationship("Webpage", backref='websites')
    url = Column(String)


class Webpage(Base):
    __tablename__ = 'webpages'
    id = Column(Integer, primary_key=True)
    website_id = Column(Integer, ForeignKey('websites.id'))
    paragraphs = relationship("Paragraph", backref='webpages')
    url = Column(String)
    crawl_datetime = Column(DateTime)
    datetime = Column(DateTime)
    published_time = Column(DateTime)
    expiration_time = Column(DateTime)
    title = Column(String)
    description = Column(String)
    author = Column(String)
    section = Column(String)
    tag = Column(String)
    keywords = Column(String)

    def __repr__(self):
       return "<Title(title={}, date crawled={})>".format(
                            self.title, str(self.datetime))


class Paragraph(Base):
    __tablename__ = 'paragraphs'
    id = Column(Integer, primary_key=True)
    webpage_id = Column(Integer, ForeignKey('webpages.id'))
    paragraph = Column(String, nullable=False)


def create_all():
    Base.metadata.create_all(engine)


def clear_all():
    for filename in [DATABASE_FILENAME, LOG_FILENAME]:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass


if __name__ == '__main__':
    clear_all()
    create_all()
