#!/usr/bin/python
#coding=utf-8
"""
Crawler for www.daodao.com , it collects all attraction details and store into 
database
"""
import re
import sys
import time
import mechanize
import sqlalchemy
import multiprocessing
from BeautifulSoup import BeautifulSoup
from sqlalchemy.orm import sessionmaker
from optparse import make_option

from hanz.cmd import BaseCommand
from crawler.daodao.model import Attraction, Link2Detail
from crawler.daodao.db import create_table, DB_CONN_URL

LAT_LNG_PATTERN = re.compile(r'lat: (-?\d+[.]\d+),\nlng: (-?\d+[.]\d+)')
completed = False

def not_start_with_tag(tag_name):
    def check(string):
        string = str(string).strip()
        return not string.startswith('<%s'%tag_name) and not string.startswith('<%s'%tag_name.upper()) and not string.startswith('<%s>'%tag_name) and not string.startswith('<%s'%tag_name.upper())
    return check

class StoreDetailCrawler(multiprocessing.Process):
    """
    Given a list of urls, StoreDetailCrawler will try to collect information based on 
    specific page format on kendi.koubei.com attraction detail page. It then stores these 
    detailed information into database, including:
         店名
         详细地址
         好评率
         点评数
         电话
         *人均
         *特色
         经纬度
         网友tag
         网友推荐
         * indicates this property is optional
    """

    link = 'http://beijing.koubei.com/store/detail--storeId-66105fd7767e4211b3b653a0fd2676c6'
    def __init__(self, pool_size=1, interval=5, writeback=True, *args, **kwargs):
        super(StoreDetailCrawler, self).__init__(*args, **kwargs)
        assert type(interval) is int
        self.interval = interval
        self.writeback = writeback
        db = sqlalchemy.create_engine(DB_CONN_URL, encoding="utf8")
        self.__db__ = sessionmaker(bind=db, autocommit=True)()

        assert type(pool_size) is int
        assert pool_size > 0

        self.link_list = self.__db__.query(Link2Detail).filter_by(status='NEW')[:pool_size]
        # completed all jobs
        if self.link_list is None or self.link_list == []:
            global completed
            completed = True
            sys.exit(0)

        # mark for 'SCRAPING'
        for link_obj in self.link_list:
            link_obj.status = 'SCRAPING'
            self.__db__.merge(link_obj)
            print "Plan to SCRAPING: %s" %(link_obj.name)

        self.name = self.link_list[0].name
        self.link = self.link_list[0].link
        self.br = mechanize.Browser()
        self.br.addheaders = [('User-Agent','Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11')]
        self.attraction_list = []
    
    def input_handler(tag):
        result_dict = {}
        full_name_input = tag.find('input',{'type':'hidden', id:'store-full-name'})
        if full_name_input: 
            store_full_name = dict(full_name_input.attrs)['value']
            result_dict['full_name'] = store_full_name

        tel_input = tag.find('input',{'type':'hidden',id:'store-full-name'})
        if tel_input: 
            store_tel = dict(tel_input.attrs)['value']
            result_dict['tel'] = store_tel

        addr_input = tag.find('input',{'type':'hidden',id:'store-address'})
        if tel_input: 
            store_address = dict(addr_input.attrs)['value']
            result_dict['address'] = store_address
        return result_dict


    def named_handler(name):
        def text_handler(tag):
            text = BeautifulSoup("".join(map(str, filter(not_start_with_tag('h3'), tag.contents)))).text
            return {name: text}
        return text_handler
    
    def address_handler(tag):
        locality_span = tag.find('span',{'class':'locality'})
        country_span = tag.find('span',{'class':'country-name'})
        street_address = tag.find('span',{'class':'street-address'})

        result_dict = {}
        if locality_span: result_dict['locality'] = locality_span.text
        if country_span: result_dict['country'] = country_span.text
        if street_address: result_dict['street_addr'] = street_address.text
        return result_dict

    def direction_handler(tag):
        if tag.find('div'):
            direction = tag.find('span',{'class':'full'}).text
        else:
            direction = "".join(map(str, filter(not_start_with_tag('h3'), tag.contents)))
        if direction is None:
            return None
        return {'direction':direction.strip()}

    def phone_url_handler(tag):
        url_a = tag.find('a')
        phone_content = filter(not_start_with_tag('a'), tag.contents)
        phone_content = filter(not_start_with_tag('h3'), phone_content)
        phone = "".join(map(str,phone_content)).strip()

        result_dict = {'phone':phone}
        if url_a: result_dict['url'] = dict(url_a.attrs)['href']
        return result_dict

    li_handler_dict = {
        # handlers for certain pattern, each handler takes in one BeautifulSoup.Tag object
        '景点类型':named_handler('category'),
        '详细地址':address_handler,
        '电话网址':phone_url_handler,
        '开放时间':named_handler('hours'),
        '门票价格':named_handler('price'),
        '交通路线':direction_handler,
    }

    def crawl(self, link):
        ''' Parse target page and form a dictionary '''

        page = BeautifulSoup(self.br.open(link).read().decode('utf-8'))
        name = page.find('h1',{'id':'HEADING'}).text
        self.name = name

        print "NAME | %s \nLINK | %s"% (self.name, self.link)
        print "URL  | %s" %self.br.geturl()
        properties = {
            'n_comments': 0,
            'rating': -1,
            'name': name,
            'link': self.link,
        }

        # Number of Comments 评论数量
        n_comments_strong = page.find('strong',{'property':'v:count'})
        if n_comments_strong: properties['n_comments'] = int(n_comments_strong.text)

        # Rating 评分
        detail_div = page.find('div', {'class':'ar-detail'})
        li_list = detail_div.findAll('li')
        rating_strong = li_list[0].find('strong')
        if rating_strong: properties['rating'] = float(rating_strong.text)
        
        # Map (GPS latitude/longtitude)
        match = LAT_LNG_PATTERN.search(page.text)
        if match:
            properties['latitude'] = float(match.group(1))
            properties['longtitude'] = float(match.group(2))

        # Grade 景区评级
        grade_span = page.find('span',{'class':'ar-grade'})
        if grade_span: properties['grade']=grade_span.text

        # Introduction 简介
        intro_div = page.find('div',{'class':'review-intro'})
        if intro_div:
            intro_content = filter(not_start_with_tag('a'), intro_div.findChild('p').contents)
            description = "".join(map(str,intro_content))
            properties['description'] = description.strip()

        # RSS 
        rss_link = page.find('link',{'type':'application/rss+xml'})
        if rss_link:
            rss_url = dict(rss_link.attrs)['href']
            properties['rss_url'] = rss_url

        for li in li_list[1:]:
            key = li.find('h3').text.strip()[:4].encode('utf-8')
            # if key in handler dict, use corresponding handler to handle
            # otherwise, return None
            # print "KEY:",key
            result_dict = self.li_handler_dict.setdefault(key, lambda x: None)(li)
            if result_dict:
                properties.update(result_dict)
        self.attraction_list.append(properties)
        return properties

    def store(self, properties):
        if properties is None:
            raise Exception('properties is None')

        p = {}
        for k, v in properties.items():
            # print "%-10s => %s" %(k,v)
            if type(v) is unicode:
                p[k] = v.encode('utf-8')
            else:
                p[k] = v

        attr = Attraction(p)
        # If attraction not exists, add it
        if self.__db__.query(Attraction).filter_by(link=attr.link).first() is None:
            self.__db__.add(attr)

        # If link exists, update status to SCRAPED
        self.link_obj.status = 'SCRAPED'
        self.__db__.merge(self.link_obj)
        print "SAVED"

    def run(self,  *args, **kwargs):
        for link_obj in self.link_list:
            start = time.time()
            self.link_obj = link_obj
            self.link = link_obj.link
            self.name = link_obj.name
            
            if not self.link.startswith("www.daodao.com") and not self.link.startswith("http://www.daodao.com"):
                link = "http://www.daodao.com"+self.link
            try:
                self.properties = self.crawl(link)
                # store result into database
                if self.writeback:
                    self.store(self.properties)
            except KeyboardInterrupt:
                link_obj.status = 'NEW'
                self.__db__.merge(link_obj)
            except:
                link_obj.status = 'ERROR'
                self.__db__.merge(link_obj)
                raise
            finally:
                for link_obj in self.link_list:
                    if link_obj.status == 'SCRAPING':
                        link_obj.status = 'NEW'
                        self.__db__.merge(link_obj)

            # sleep to meet rate limit
            end = time.time()

            sleep_sec = 0
            if end-start<self.interval:
                sleep_sec = self.interval-(end-start)

            print "USED | %.2f sec, gonna sleep for %.2f sec" % (end-start, sleep_sec)
            print '-'*80; sys.stdout.flush()
            time.sleep(sleep_sec)

        self.__db__.close()

class Command(BaseCommand):
    option_list = BaseCommand.option_list + [
        make_option('--fresh', action='store_true', dest='fresh', default=False,
                   help='fresh start'),
        make_option('--once', action='store_true', dest='once', default=False,
                   help='run only once'),
        make_option('--size', action='store', type='int', dest='size', default=10,
                    help='number of links the crawler will try to crawl'),
        make_option('--interval', action='store', type='int', dest='interval', default=5,
                    help='crawling frequency control, 1 link will be crawled in at least ``interval`` seconds'),
    ]

    def __init__(self):
        super(Command, self).__init__()

    def execute(self, *args, **options):
        if options['fresh']:
            choice = raw_input("WARNING! you are about to remove all data in Table daodao.attraction, are you sure you want to continue? [yes/NO]")
            if (choice=='yes'):
                db = sqlalchemy.create_engine(DB_CONN_URL, encoding="utf8")
                create_table(db, Attraction, drop_before_create=True)

        num2crawl = options['size']
        interval = options['interval']
        if options['once']:
            # run once, test mode (no writeback)
            p = StoreDetailCrawler(interval=interval, pool_size=num2crawl, writeback=False)
            p.start()
            p.join()
        else:
            # run serial
            while(not completed):
                p = StoreDetailCrawler(pool_size=num2crawl, interval=interval)
                p.start()
                p.join()

if __name__=="__main__":
    cmd = Command()
    cmd.run_from_argv(sys.argv)
