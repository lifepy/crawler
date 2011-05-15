from sqlalchemy import *
from sqlalchemy.orm import relationship

from crawler.model import Link2List, Link2Detail, Page, Attribute, Base

store_tag_table = Table(
    'store_tag', Base.metadata,
    Column('store_id', Integer, ForeignKey('store.id')),
    Column('tag_id', Integer, ForeignKey('tag.id')),
)
store_impress_table = Table(
    'store_impress', Base.metadata,
    Column('store_id', Integer, ForeignKey('store.id')),
    Column('impress_id', Integer, ForeignKey('impress.id')),
)
store_promote_table = Table(
    'store_promote', Base.metadata,
    Column('store_id', Integer, ForeignKey('store.id')),
    Column('promote_id', Integer, ForeignKey('promote.id')),
)

class Store(Base):
    __tablename__ = "store"
    id = Column(Integer, primary_key=True)
    
    name = Column(String(100))
    feature = Column(String(100))
    description = Column(Text(2000))

    phone = Column(String(100))
    rating = Column(Float(precision=2))
    category = Column(String(100))
    n_comments = Column(Integer)
    address = Column(String(200))
    avg_cost = Column(String(100))

    url = Column(String(200)) # url to this store
    link = Column(String(300)) # link to this store at koubei.com

    tags = relationship("Tag", secondary=store_tag_table, backref='stores')
    impress = relationship("Impress", secondary=store_impress_table, backref='stores')
    promote = relationship("Promote", secondary=store_promote_table, backref='stores')
    cooking_variety = Column(String(100))

    link = Column(Text(200))

    def __init__(self, props):
        """ takes in an dictionary with <k,v> items, where
        k is the name of property
        v is the value of property
        """
        if props.has_key('tags'):
            for tag in props['tags']:
                self.tags.append(Tag(tag))
            del props['tags']

        if props.has_key('impress'):
            for imp in props['impress']:
                self.impress.append(Impress(imp))
            del props['impress']

        if props.has_key('promote'):
            for pro,occ in props['promote'].items():
                self.promote.append(Promote(pro,occ))
            del props['promote']

        for k, v in props.items():
            self.__setattr__(k,v)

    def __repr___(self):
        return "%d, %s %s %s" % (self.id, self.name, self.address, self.category)

class Tag(Attribute):
    __tablename__= "tag"
    __mapper_args__ = {'polymorphic_identity':'tag'}
    id = Column(Integer, ForeignKey('attribute.id'), primary_key=True)

class Impress(Attribute):
    __tablename__= "impress"
    __mapper_args__ = {'polymorphic_identity':'impress'}
    id = Column(Integer, ForeignKey('attribute.id'), primary_key=True)

class Promote(Attribute):
    __tablename__='promote'
    __mapper_args__ = {'polymorphic_identity':'promote'}
    id = Column(Integer, ForeignKey('attribute.id'), primary_key=True)
