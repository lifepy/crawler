import datetime
from sqlalchemy import Column, Integer, Enum, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

conn_url = 'mysql+mysqldb://crawler:test@orca.rcac.purdue.edu:3306/test'
Base = declarative_base()

# Link
class MixinLink(object):
    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    url = Column(String(300), unique=True)
    status = Column(Enum('NEW','SCRAPING','SCRAPED','ERROR'), default='NEW')
    last_update = Column(DateTime, onupdate=datetime.datetime.now, default=datetime.datetime.now)

class Link2List(Base, MixinLink):
    __tablename__='link2list'
    def __init__(self, name, url):
        self.name = name
        self.url = url

class Link2Detail(Base, MixinLink): 
    __tablename__='link2detail'
    def __init__(self, name, url):
        self.name = name
        self.url = url

# Page
class MixinPage(object):
    id = Column(Integer, primary_key=True)
    url = Column(String(300), unique=True)
    content = Column(Text(70000))
    last_update = Column(DateTime, onupdate=datetime.datetime.now, default=datetime.datetime.now)

class Page(Base, MixinPage):
    __tablename__='page'
    def __init__(self, url, content):
        self.url = url
        self.content = content

# Attribute [polymorphism enabled]
class MixinAttribute(object):
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    occurence = Column(Integer, default=1)
    sub_type = Column(String(50), default='attribute')
    __mapper_args__={'polymorphic_on':sub_type}

class Attribute(Base, MixinAttribute):
    __tablename__ = "attribute"
    def __init__(self, name, occurrence=1):
        self.name = name
        self.occurrence = occurrence
