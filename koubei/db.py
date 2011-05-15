#!/usr/bin/python
# encoding=utf8
import sqlalchemy
import urllib2
from BeautifulSoup import BeautifulSoup
from sqlalchemy.orm import sessionmaker
from optparse import make_option

from xpinyin.xpinyin import Pinyin

from crawler.db import create_table
from crawler.cmd import DatabaseBaseCommand
from crawler.koubei.model import Store, Link2List, Link2Detail, Page, Attribute, Tag, Impress, Promote, Base

db_server = 'orca.rcac.purdue.edu'
db_name = 'koubei'
db_username = 'koubei'
db_password = 'crawler'

DB_CONN_URL='mysql+mysqldb://%s:%s@%s:3306/%s' % (db_username, db_password, db_server, db_name)
SEEDS_FILE = 'seeds.txt'

def get_base_url_dict():
    # read seed html
    with open('selectcitynew.html','r') as f:
        page = f.read().decode('utf-8')

    def is_valid_url(name, url):
        try:
            p = urllib2.urlopen(url).read()
            p = BeautifulSoup(p)
            page_name = p.find('strong',{'class':'name'}).text
            if name == page_name:
                print '+',name, page_name
                return True
            else:
                return False
        except:
            return False
 
    # parse and generate a list
    page = BeautifulSoup(page)
    p = Pinyin()
    link2list = {}
    for a in page.findAll('a', {'href':'javascript:void(0)'}):
        if not a.text.startswith("显示更多".decode('utf-8')):
            name = a.text
            id = a['id']
            pinyin = p.get_pinyin(name.encode('utf-8'))
            id_url = 'http://bendi.koubei.com/c%s/searchstore' % id
            pinyin_url = 'http://bendi.koubei.com/%s/searchstore' % pinyin

            if is_valid_url(name,pinyin_url):
                link2list[name] = pinyin_url
            elif is_valid_url(name,id_url):
                link2list[name] = id_url
            if name in link2list:
                print '+ %s  %s' % (name, link2list[name])

    print '-'*80
    with open(SEEDS_FILE,'w') as o:
        for k, v in link2list.items():
            print k, '  ',v
            o.write(k.encode('utf-8')+','+v.encode('utf-8')+'\n')

    return link2list

# DB Initialization
def syncdb(db_uri):
    db = sqlalchemy.create_engine(db_uri, encoding="utf8", echo=True)
    Base.metadata.create_all(db)

def init_db_for_collect(db_uri):
    db = sqlalchemy.create_engine(db_uri, encoding="utf8", echo=True)
    create_table(db, Link2List)
    create_table(db, Link2Detail)
    create_table(db, Page)

    Session = sessionmaker(bind=db)
    session = Session()

    with open(SEEDS_FILE, 'r') as f:
        for line in f.readlines():
            name, link = line.strip().split(',')
            session.add(Link2List(name, link))

    session.commit()
    session.close()

def init_db_for_crawl(db_uri):
    db = sqlalchemy.create_engine(db_uri, encoding="utf8", echo=True)
    create_table(db, Store)
    create_table(db, Attribute)
    create_table(db, Tag)
    create_table(db, Impress)
    create_table(db, Promote)

class Command(DatabaseBaseCommand):
    option_list = DatabaseBaseCommand.option_list + [
        make_option('--init-seed',action='store_true',dest='init-seed',default=False,
                   help="read in selectcitynew.html and generate '%s'" % SEEDS_FILE),
    ]

    def execute(self, *args, **options):
        if options['syncdb']:
            syncdb(DB_CONN_URL)

        if options['init-seed']:
            get_base_url_dict()

        if options['init-collect']:
            init_db_for_collect(DB_CONN_URL)

        if options['init-crawl']:
            init_db_for_crawl(DB_CONN_URL)

if __name__=="__main__":
    import sys
    cmd = Command()
    cmd.run_from_argv(sys.argv)
