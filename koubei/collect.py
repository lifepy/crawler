#!/usr/bin/python
#coding=utf-8
import sys
import time
import urllib2
from BeautifulSoup import BeautifulSoup
import multiprocessing
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from optparse import make_option

from crawler.cmd import BaseCommand
from crawler.koubei.model import Link2List, Link2Detail, Page
from crawler.koubei.db import init_db_for_collect, DB_CONN_URL, exist_page, save_page

firefox_headers = { 
    "User-Agent":"Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11",
    "Accept":"*/*",
    "Host":"jingqu.travel.sohu.com"
}

class LinkSpider(multiprocessing.Process):
    """
    Follow "下一页" to scrape all useful links, two typies of link are of interest:
        1. inside <div class="geoList">  : links for list of attraction page
        2. inside <div class="attraction-list clearfix"> : links for attraction detail page
    """
    def __init__(self, name, url, *args, **kwargs):
        super(LinkSpider, self).__init__(*args, **kwargs)
        self.name = name
        self.__url__ = url
        self.link_dict = {}
        db = sqlalchemy.create_engine(DB_CONN_URL, encoding="utf8")
        self.__db__ = sessionmaker(bind=db, autocommit=True)()

    def run(self, *args, **kwargs):
        while(True):
            print "FOLLOWING: %s" %self.__url__
            try:
                content = self.get_page_content(self.__url__)
            except Exception as e:
                print e
                if 'Moved Permanently' not in str(e):
                    break

            link2follow, links2detail = self.parse(content)

            if link2follow and not link2follow.startswith('http://bendi.koubei.com'):
                self.__last_visit_url__ = self.__url__
                self.__url__ = 'http://bendi.koubei.com'+link2follow
                self.store(links2detail)
            else:
                break
        else:
            print "please use set `__url__` to as a starting point before running"

    def parse(self, content):
        '''
        Parse page, extract links into links2detail
        content: unicode string of the page that would be parsed
        '''
        is_last_page = False
        page = BeautifulSoup(content)

        # Is last page?
        page_end_span = page.find('span',{'class':'page-end'})
        if page_end_span:
            is_last_page = True

        next_page_a = page.find('a',{'class':'next-page'})
        if next_page_a: 
            link2follow = next_page_a['href']

        page_num_span = page.find('span',{'class':'page-num'})
        if page_num_span:
            page_num = page_num_span.text
            print "PAGE:", page_num

        links2detail = {}
        for store_name_h2 in page.findAll('h2',{'class':'store-name'}):
            store_a = store_name_h2.find('a')
            store_link = store_a['href']
            store_name = store_a.text
            links2detail[store_link] = store_name

        if is_last_page:
            return (None,None)
        else:
            return (link2follow, links2detail)
 
    def get_page_content(self, url):
        page_obj = self.__db__.query(Page).filter_by(link=url).first()
        if page_obj:
            return page_obj.content.decode('utf-8')
        else:
            try:
                # request = urllib2.Request(url, headers=firefox_headers)
                # content = urllib2.urlopen(request).read().decode('gbk')
                content = urllib2.urlopen(url).read().decode('gbk')
            except Exception as e:
                print e
                print 'error happened'

            self.__db__.add(Page(self.__url__, content.encode('utf-8')))
            return content

    def store(self, links2detail):
        # store links2detail to database
        self.__db__.begin()
        for link, name in links2detail.items():
            link = link.encode('utf-8')
            name = name.encode('utf-8')
            if self.__db__.query(Link2Detail).filter_by(link=link).first() is None:
                print "+A %-18s %s" %(name, link)
                self.__db__.add(Link2Detail(name, link))
                
        # update scraped status for __last_visit_url
        link_obj = self.__db__.query(Link2List).filter_by(link=self.__last_visit_url__).first()
        if link_obj is None:
            self.__db__.add(Link2List("",self.__last_visit_url__, True))
        else:
            link_obj.scraped = True
            self.__db__.merge(link_obj)

        self.__db__.commit()

def run_pool(pool_size=10):
    db = sqlalchemy.create_engine(DB_CONN_URL)
    Session = sessionmaker(bind=db)
    session = Session()
    while(session.query(Link2List).filter_by(scraped=False).all()):
        rs = session.query(Link2List).filter_by(scraped=False).all()
        max_index = len(rs) - 1
        nxt_index = 0

        while(nxt_index<=max_index):
            add_n = min(pool_size-len(multiprocessing.active_children()), max_index-nxt_index+1)

            for i in range(add_n):
                r = rs[nxt_index+i]
                p = LinkSpider(r.name, r.link)
                p.start()
            nxt_index += add_n

            time.sleep(5)

        while(multiprocessing.active_children()):
            time.sleep(8)

def run_one(link):
    p = LinkSpider("TEST", link)
    p.start()
    p.join()

class Command(BaseCommand):
    option_list = BaseCommand.option_list + [
        make_option('--fresh', action='store_true', dest='fresh', default=False),
        make_option('--link', action='store', dest='link'),
        make_option('--pool-size', action='store', type='int',dest='pool-size',default=10),
    ]

    def __init__(self):
        super(Command, self).__init__()
        print self.option_list

    def execute(self, *args, **options):
        # re-generate database scheme
        if options['fresh']:
            choice = raw_input('''You are about to remove all data in\n\tkoubei.store\nAre you sure you want to do that? [yes/NO]''')
            if (choice == 'yes'):
                init_db_for_collect(DB_CONN_URL)

        # pool size option
        pool_size = 10
        if options['pool-size']:
            pool_size = options['pool-size']

        # link option
        if options['link']:
            run_one(options['link'])
        else:
            run_pool(pool_size)

if __name__=="__main__":
    cmd = Command()
    cmd.run_from_argv(sys.argv)
