# encoding=utf8
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from optparse import make_option

from hanz.cmd import BaseCommand
from crawler.daodao.model import Attraction, Link2List, Link2Detail

db_server = 'orca.rcac.purdue.edu'
db_name = 'daodao'
db_username = 'crawler'
db_password = 'daodao'

DB_CONN_URL='mysql+mysqldb://%s:%s@%s:3306/%s' % (db_username, db_password, db_server, db_name)

def drop_table(engine, declare_class):
    table = declare_class.__table__
    table.metadata.bind = engine
    if table.exists():
        table.drop()
        "DROPPED TABLE: '%s'" %declare_class.__tablename__
    else:
        "TABLE '%s' NOT EXIST" %declare_class.__tablename__
 
def create_table(engine, declare_class, drop_before_create=False):
    if drop_before_create:
        drop_table(engine, declare_class)

    table = declare_class.__table__
    table.metadata.bind = engine
    if not table.exists():
        table.create()
        print "CREATED TABLE: '%s'" %declare_class.__tablename__
    else:
        print "TABLE '%s' EXISTS" %declare_class.__tablename__


base_url_dict= {
    'Asia': '/Attractions-g2-Activities-Asia.html',
    'Europe': '/Attractions-g4-Activities-Europe.html',
    'Africa': '/Attractions-g6-Activities-Africa.html',
    'Australia': '/Attractions-g8-Activities-South_Pacific.html',
    'South America': '/Attractions-g13-Activities-South_America.html',
    # North America
    'USA': '/Attractions-g191-Activities-United_States.html',
    'Canada': '/Attractions-g153339-Activities-Canada.html',
    'Mexico': '/Attractions-g150768-Activities-Mexico.html',
    'Carribean': '/Attractions-g147237-Activities-Caribbean.html',
}

# DB Initialization
def init_db_for_collect(db_uri):
    db = sqlalchemy.create_engine(db_uri, encoding="utf8", echo=True)
    create_table(db, Link2List, drop_before_create=True)
    create_table(db, Link2Detail, drop_before_create=True)

    Session = sessionmaker(bind=db)
    session = Session()

    for name,link in base_url_dict.items():
        session.add(Link2List(name, link))

    session.commit()
    session.close()

def init_db_for_crawl(db_uri):
    db = sqlalchemy.create_engine(db_uri, encoding="utf8", echo=True)
    create_table(db, Attraction, drop_before_create=True)

class Command(BaseCommand):
    option_list = BaseCommand.option_list + [
        make_option('--init-collect', action='store_true', dest='collectdb', default=False,
                    help='initiate database for collecting links'),
        make_option('--init-crawl', action='store_true', dest='crawldb', default=False,
                    help='initiate database for crawling detail info'),
    ]
    def execute(self, *args, **options):
        if options['init-collect']:
            # clean up and recreate db for collect
            choice = raw_input('''WARNING! You are about to remove ALL data in 
                                   daodao.link2detail, daodao.link2list 
                               Are you sure you want to do that? [yes/NO]''')
            if (choice=='yes'):
                init_db_for_collect(DB_CONN_URL)

        if options['init-crawl']:
            # clean up and recreate db for crawl
            choice = raw_input('''WARNING! You are about to remove ALL data in 
                                    daodao.attraction
                               Are you sure you want to do that? [yes/NO]''')
            if (choice=='yes'):
                init_db_for_crawl(DB_CONN_URL)
