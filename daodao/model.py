import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Link2List(Base):
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String(200))
    link = sqlalchemy.Column(sqlalchemy.String(300), unique=True)
    scraped = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    __tablename__='link2list'
    __mapper_args__={'primary_key':id}

    def __init__(self, name, link, scraped=False):
        self.name = name
        self.link = link
        self.scraped = scraped

class Link2Detail(Base):
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String(200))
    link = sqlalchemy.Column(sqlalchemy.String(300), unique=True)
    status = sqlalchemy.Column(sqlalchemy.Enum('NEW','SCRAPING','ERROR','SCRAPED', default='NEW'))
    __tablename__='link2detail'
    __mapper_args__={'primary_key':id}

    def __init__(self, name, link, scraped=False):
        self.name = name
        self.link = link
        self.scraped = scraped

class Attraction(Base):
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String(100))
    rating = sqlalchemy.Column(sqlalchemy.Float(precision=2))
    category = sqlalchemy.Column(sqlalchemy.String(100))
    grade = sqlalchemy.Column(sqlalchemy.String(20))
    n_comments = sqlalchemy.Column(sqlalchemy.Integer)

    country = sqlalchemy.Column(sqlalchemy.String(50))
    locality = sqlalchemy.Column(sqlalchemy.String(50))
    street_addr = sqlalchemy.Column(sqlalchemy.String(150))

    phone = sqlalchemy.Column(sqlalchemy.String(100))
    url = sqlalchemy.Column(sqlalchemy.String(200))
    hours = sqlalchemy.Column(sqlalchemy.String(100))
    price = sqlalchemy.Column(sqlalchemy.String(100))

    latitude = sqlalchemy.Column(sqlalchemy.Float(precision=8))
    longtitude = sqlalchemy.Column(sqlalchemy.Float(precision=8))

    direction = sqlalchemy.Column(sqlalchemy.Text(500))
    description = sqlalchemy.Column(sqlalchemy.Text(1000))

    rss_url = sqlalchemy.Column(sqlalchemy.String(50))
    link = sqlalchemy.Column(sqlalchemy.Text(200))

    __tablename__ = "attraction"
    __mapper_args = {'primary_key':id}

    def __init__(self, properties_dict):
        """ takes in an dictionary with <k,v> items, where
        k is the name of property
        v is the value of property
        """
        for k, v in properties_dict.items():
            self.__setattr__(k,v)

    def __repr___(self):
        return "%d, %s, %s %s %s" % (self.id, self.name, self.country, self.locality, self.street_addr)

