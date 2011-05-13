#!/usr/bin/python
# encoding=utf8
import sys
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from optparse import make_option

from crawler.db import create_table
from crawler.cmd import BaseCommand
from crawler.daodao.model import Attraction, Link2List, Link2Detail

db_server = 'orca.rcac.purdue.edu'
db_name = 'daodao'
db_username = 'crawler'
db_password = 'daodao'

DB_CONN_URL='mysql+mysqldb://%s:%s@%s:3306/%s' % (db_username, db_password, db_server, db_name)
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

def list_category(db_uri):
    db = sqlalchemy.create_engine(db_uri, encoding="utf8", echo=True)
    Session = sessionmaker(bind=db)
    session = Session()
    
    rs = session.query(Attraction).all()
    category = {}
    for a in rs:
        if a.category in category.keys():
            category[a.category] += 1
            # print "+ %s" %a.category
        else:
            category[a.category] = 1
            # print '- %s' %a.category

    detailed_category = {}
    for cat,count in category.items():
        if cat is None:
            detailed_category[cat] = count
        else:
            categories = cat.split(',')
            for c in categories:
                if c in detailed_category.keys():
                    detailed_category[c] += count
                else:
                    detailed_category[c] = count

    for k,v in detailed_category.items():
        print k,'--',v

    print "TOTAL:",len(detailed_category)
    
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
        make_option('--init-collect', action='store_true', dest='init-collect', default=False,
                    help='initiate database for collecting links'),
        make_option('--init-crawl', action='store_true', dest='init-crawl', default=False,
                    help='initiate database for crawling detail info'),
        make_option('--list-category', action='store_true', dest='list-category', default=False,
                    help='list all categories'),
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

        if options['list-category']:
            list_category(DB_CONN_URL)

if __name__=="__main__":
    cmd = Command()
    cmd.run_from_argv(sys.argv)
