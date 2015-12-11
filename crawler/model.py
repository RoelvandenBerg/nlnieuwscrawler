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

try:
    from settings import *
except ImportError:
    from crawler.settings import *

engine = create_engine('sqlite:///' + DATABASE_FILENAME, echo=VERBOSE)

Base = declarative_base()

Session = sessionmaker(bind=engine)


class Website(Base):
    __tablename__ = 'websites'
    id = Column(Integer, primary_key=True)
    webpages = relationship("Webpage", backref='websites')
    created = Column(DateTime)
    modified = Column(DateTime)
    crawl_depth = Column(Integer)
    url = Column(String)


class Webpage(Base):
    __tablename__ = 'webpages'
    id = Column(Integer, primary_key=True)
    website_id = Column(Integer, ForeignKey('websites.id'))
    url = Column(String)
    crawl_created = Column(DateTime)
    crawl_modified = Column(DateTime)
    content = Column(String)
    paragraphs = relationship("Paragraph", backref='webpages')
    headings = relationship("Heading", backref='webpages')
    revisit = Column(String)
    published_time = Column(DateTime)
    modified_time = Column(DateTime)
    expiration_time = Column(DateTime)
    title = Column(String)
    description = Column(String)
    author = Column(String)
    section = Column(String)
    tag = Column(String)
    keywords = Column(String)

    def __repr__(self):
       return "<Title(title={}, date crawled={})>".format(
                            self.title, str(self.crawl_modified))


class Paragraph(Base):
    __tablename__ = 'paragraphs'
    id = Column(Integer, primary_key=True)
    webpage_id = Column(Integer, ForeignKey('webpages.id'))
    headings_id = Column(Integer, ForeignKey('headings.id'))
    paragraph = Column(String, nullable=False)


class Heading(Base):
    __tablename__ = 'headings'
    id = Column(Integer, primary_key=True)
    webpage_id = Column(Integer, ForeignKey('webpages.id'))
    paragraphs = relationship("Paragraph", backref='headings')
    h1 = Column(String)
    h2 = Column(String)
    h3 = Column(String)
    h4 = Column(String)
    h5 = Column(String)
    h6 = Column(String)


def create_all():
    Base.metadata.create_all(engine)


def clear_all():
    for filename in [DATABASE_FILENAME, LOG_FILENAME]:
        try:
            os.remove(filename)
        except:
            pass


if __name__ == '__main__':
    print('clear all')
    clear_all()
    print('create_all')
    print('sqlite:///' + DATABASE_FILENAME)
    create_all()
