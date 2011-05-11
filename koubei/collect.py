#!/usr/bin/python
#coding=utf-8
import sys
import time
from BeautifulSoup import BeautifulSoup
import mechanize as M
import multiprocessing
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from optparse import make_option

from crawler.cmd import BaseCommand
from crawler.daodao.model import Link2List, Link2Detail
from crawler.daodao.db import init_db_for_collect, DB_CONN_URL

POOL_SIZE=50

class LinkSpider(multiprocessing.Process):
    """
    Follow "下一页" to scrape all useful links, two typies of link are of interest:
        1. inside <div class="geoList">  : links for list of attraction page
        2. inside <div class="attraction-list clearfix"> : links for attraction detail page
    """
    def __init__(self, name, base_url, *args, **kwargs):
        super(LinkSpider, self).__init__(*args, **kwargs)
        self.name = name
        self.base_url = base_url
        self.link_dict = {}
        self.db = sqlalchemy.create_engine(DB_CONN_URL)

    def run(self, *args, **kwargs):
        if self.base_url:
            self.follow()
            #self.store()
        else:
            print "please use set `base_url` to as a starting point before running"

    def follow(self):
        br = M.Browser()
        br.open(self.base_url)
        print "FOLLOWING: %s" %br.geturl()
        is_last_page = False
        while(br.viewing_html()):
            try:
                page = BeautifulSoup(br.response().read().decode('gbk'))

                # Is last page?
                page_end_span = page.find('span',{'class':'page-end'})
                if page_end_span: is_last_page = True

                next_page_a = page.find('a',{'class':'next-page'})
                if next_page_a: 
                    link2follow = dict(next_page_a.attrs)['href']

                page_num_span = page.find('span',{'class':'page-num'})
                if page_num_span:
                    page_num = page_num_span.text
                    print "PAGE:", page_num

                for store_name_h2 in page.findAll('h2',{'class':'store-name'}):
                    store_a = store_name_h2.find('a')
                    link2detail = dict(store_a.attrs)['href']
                    print "D: %s" %link2detail
                
                if is_last_page: break
                print "LINK2FOLLOW: %s" % link2follow
                for link in br.links():
                    print link.url
                    if link2follow in link.url:
                        link2follow = link

                br.follow_link(link=link2follow)
                print "FOLLOING: %s" %br.geturl()
            except M.LinkNotFoundError:
                # Stop following
                break

    def store(self):
        # store self.link_dict to database
        Session = sessionmaker(bind=self.db)
        session = Session()
        for link, name in self.link_dict.items():
            if self.is_geo_list:
                if session.query(Link2List).filter_by(link=link).first() is None:
                    print "+ %-18s %s" %(name, link)
                    session.add(Link2List(name, link))
            else:
                if session.query(Link2Detail).filter_by(link=link).first() is None:
                    print "+A %-18s %s" %(name, link)
                    session.add(Link2Detail(name, link))
                
        print "OK"

        # update scraped status for current page
        self_link = self.base_url[self.base_url.rindex('/'):]
        link_obj = session.query(Link2List).filter_by(link=self_link).first()
        if link_obj is None:
            session.add(Link2List("",self_link, True))
        else:
            link_obj.scraped = True
            session.merge(link_obj)
        session.commit()
        session.close()

    def set_base_url(self, base_url=None):
        self.base_url = base_url

def run_pool():
    db = sqlalchemy.create_engine(DB_CONN_URL)
    Session = sessionmaker(bind=db)
    session = Session()
    while(session.query(Link2List).filter_by(scraped=False).all()):
        rs = session.query(Link2List).filter_by(scraped=False).all()
        max_index = len(rs) - 1
        nxt_index = 0

        while(nxt_index<=max_index):
            add_n = min(POOL_SIZE-len(multiprocessing.active_children()), max_index-nxt_index+1)

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
        make_option('--link', action='store', dest='link')
    ]

    def __init__(self):
        super(Command, self).__init__()
        print self.option_list

    def execute(self, *args, **options):
        if options['fresh']:
            init_db_for_collect(DB_CONN_URL)

        if options['link']:
            run_one(options['link'])
        else:
            run_pool()

if __name__=="__main__":
    cmd = Command()
    cmd.run_from_argv(sys.argv)
