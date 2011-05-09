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
        if "http://www.daodao.com" not in self.base_url and self.base_url.startswith('/'):
            self.base_url = "http://www.daodao.com"+self.base_url
        self.link_dict = {}
        self.db = sqlalchemy.create_engine(DB_CONN_URL)
        self.log_file_name = 'log/%s.log' %name
        self.is_geo_list = False

        import sys
        try:
            sys.stdout = open(self.log_file_name, 'a')
        except IOError as e:
            print e

    def run(self, *args, **kwargs):
        if self.base_url:
            self.follow()
            self.store()
        else:
            print "please use set `base_url` to as a starting point before running"

    def follow(self):
        br = M.Browser()
        br.open(self.base_url)
        print "FOLLOWING: %s" %br.geturl()
        while(br.viewing_html()):
            try:
                is_last_page = False
                page = BeautifulSoup(br.response().read().decode('utf-8'))
                pg_now_span = page.find('span',{'class':'nowpage'})
                try:
                    deck_btm_div = page.findAll('div', {'class':'deckTools btm'})[-1]
                except IndexError:
                    deck_btm_div = page.findAll('div', {'class':'bottomDeckTools clearfix'})[0]

                geo_list_ul = page.find('ul', {'class':'geoList'})
                attraction_list_divs = page.findAll('div', {'class':'attraction-list clearfix'})

                # probe if is the last page to follow
                if deck_btm_div is None:
                    is_last_page = True

                if pg_now_span:
                    pg_now =  pg_now_span.text
                    pg_total_span = deck_btm_div.findAll('a', {'class':'num '})[-1]
                    pg_total =  pg_total_span.text
                    print "PAGE: %s/%s" % (pg_now, pg_total)
                    if int(pg_now)>=int(pg_total):
                        is_last_page = True

                if (geo_list_ul):
                    # links for attraction list page
                    self.is_geo_list = True
                    for li  in geo_list_ul.findAll('li'):
                        a = li.findChild('a')
                        name = a.text.encode('utf-8')
                        link = dict(a.attrs)[u'href'].encode('utf-8')
                        self.link_dict[link] = name
                        print "+ %-16s : %s" % (name, link)
                elif (attraction_list_divs):
                    # links for attraction detail page
                    for attraction_div in attraction_list_divs:
                        title_div = attraction_div.findChild('div', {'class':'title'})
                        att_name = title_div.text.encode('utf-8')
                        att_link = dict(title_div.findChild('a').attrs)['href'].encode('utf-8')
                        self.link_dict[att_link] = att_name
                        print "+ %-16s : %s" %(att_name, att_link)
                else:
                    # no more link of interest
                    print "NOTHING TO FOLLOW"
                    
                if is_last_page: break
                br.follow_link(url_regex=r'/Attraction.+oa\d+.+', text_regex="下一页", nr=1)
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

def usage():
    print """
    USAGE
        python crawl.py [OPTION]

    OPTION
        -h, --help        print this message
        -f, --fresh       remove all existed tables and start over in parallel mode
        --link=LINK       set a start point and run the crawler in one process
    """

class Command(BaseCommand):
    usage = BaseCommand.usage+'''
    --fresh     : fresh start
    --link=LINK : run in single mode and only collect link provided'''
    
    option_list = BaseCommand.option_list + [
        make_option('--fresh', action='store_true', dest='fresh', default=False),
        make_option('--link', action='store', dest='link')
    ]

    def __init__(self):
        super(Command, self).__init__()
        print self.option_list

    def execute(self, *args, **options):
        if options.fresh:
            init_db_for_collect(DB_CONN_URL)

        if options.link:
            run_one(options.link)
        else:
            run_pool()

if __name__=="__main__":
    cmd = Command()
    cmd.run_from_argv(sys.argv)
