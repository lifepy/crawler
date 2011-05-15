#!/usr/bin/python
# encoding: utf-8
"""
Crawler for www.koubei.com , it collects all attraction details and store into 
database
"""
import re
import sys
import time
import urllib2
import sqlalchemy
import multiprocessing
from optparse import make_option
from BeautifulSoup import BeautifulSoup
from sqlalchemy.orm import sessionmaker

from crawler.cmd import CrawlerBaseCommand
from crawler.crawl import DetailBaseCrawler
from crawler.koubei.model import Link2Detail, Store, Page
from crawler.koubei.db import DB_CONN_URL, init_db_for_crawl

LAT_LNG_PATTERN = re.compile(r'lat: (-?\d+[.]\d+),\nlng: (-?\d+[.]\d+)')
RATING_PATTERN = re.compile(r'(\d+.?\d+)%')
completed = False

class StoreDetailCrawler(DetailBaseCrawler):
    """
    Given a list of urls, StoreDetailCrawler will try to collect information based on 
    specific page format on kendi.koubei.com attraction detail page. It then stores these 
    detailed information into database, including:
         店名
         分类（主营）
         特色
         手机*
         菜系*
         介绍
         地址
         网站地址
         好评率
         点评数
         电话
         人均*
         特色*
         经纬度
         标签
         *网友推荐
         *网友印象

         * indicates this property is optional
    """
    store_list = []

    def get_db_session(self):
        db = sqlalchemy.create_engine(DB_CONN_URL,encoding='utf8')
        return sessionmaker(bind=db, autocommit=True)()
    
    def get_links(self):
        return self.__db__.query(Link2Detail).filter_by(status='NEW').limit(self.count).all()

    def save_page(self, url, content):
        page_obj = self.__db__.query(Page).filter_by(url=url).first()
        if page_obj:
            page_obj.content = content.encode('utf-8')
            self.__db__.merge(page_obj)
        else:
            self.__db__.add(Page(url, content.encode('utf-8')))

    def fetch_from_db(self, url):
        page_obj = self.__db__.query(Page).filter_by(url=url).first()
        if page_obj:
            return page_obj.content.decode('utf-8')
        else: 
            return None

    def fetch_from_web(self, url):
        return urllib2.urlopen(url).read().decode('utf-8')

    def parse(self, content):
        '''Given a unicode string of page, parse it into a dictionary'''
        page = BeautifulSoup(content)
        print "NAME | %s \nLINK | %s"% (self.name, self.url)

        properties = {
            'n_comments': 0,
            'rating': -1,
            'link': self.url,
        }

        # Address 地址
        # Name 店铺名称
        # Telephone 电话
        p = self.handle_hidden_input(page)
        properties.update(p)
        
        # Average Cost 人均
        store_info_card = page.find('div',{'class':'store-info-card'})
        if store_info_card:
            p = self.handle_store_info_card(store_info_card)
            properties.update(p)

        # Number of Comments 评论数量
        # Rating 评分
        title_div = page.find('div', {'class':'store-free-title k2-fix-float'})
        if title_div:
            p = self.handle_rating(title_div)
            properties.update(p)

        # Detail Main 详细信息
        detail_main_div = page.find('div',{'class':'detail-main'})
        if detail_main_div:
            p = self.handle_detail_main(detail_main_div)
            properties.update(p)

        # Promote 推荐
        promote_more_div = page.find('div',{'id':'promote-more'})
        if promote_more_div:
            p = self.handle_promote_more(promote_more_div)
            properties.update(p)

        # Impress 印象
        impress_more_div = page.find('div',{'id':'impress-more'})
        if impress_more_div:
            p = self.handle_impress_more(impress_more_div)
            properties.update(p)

        self.store_list.append(properties)
        return properties

    def save(self, properties):
        if properties is None:
            raise Exception('properties is None')
        self.__db__.begin()

        props = self.encode(properties)

        store = Store(props)

        # If store not exists, add it
        if self.__db__.query(Store).filter_by(link=self.url).first() is None:
            self.__db__.add(store)
            print "[STORE] %s" %store.name
        self.__db__.commit()

        link_obj = self.__db__.query(Link2Detail).filter_by(url=self.url).first()
        if link_obj:
            link_obj.status = 'SCRAPED'
            self.__db__.merge(link_obj)
            print " [LINK] %s" % link_obj.url

    def handle_store_info_card(self, div):
        props = {}
        for li in div.findAll('li'):
            if li.text.startswith('人均'.decode('utf-8')):
                props['avg_cost'] = li.text.split('：'.decode('utf-8'))[1]
            if li.text.startswith('特色'.decode('utf-8')):
                feature = li.text.split('：'.decode('utf-8'))[1]
                features = [f.strip() for f in feature.split('&nbsp;')]
                features = filter(lambda x:x !='' and x is not None, features)
                props['feature'] = ','.join(features)
        return props

    def handle_hidden_input(self, page):
        props = {}
        full_name_input = page.find('input',{'type':'hidden', 'id':'store-full-name'})
        if full_name_input: 
            store_full_name = dict(full_name_input.attrs)['value']
            props['name'] = store_full_name

        tel_input = page.find('input',{'type':'hidden','id':'store-tel'})
        if tel_input: 
            store_tel = dict(tel_input.attrs)['value']
            props['phone'] = store_tel

        addr_input = page.find('input',{'type':'hidden','id':'store-address'})
        if tel_input: 
            store_address = dict(addr_input.attrs)['value']
            props['address'] = store_address
        return props

    def handle_detail_main(self, div):
        props = {}
        for li in div.findAll('li'):
            sub_label = li.findChild('label')
            sub_div = li.findChild('div')

            #if sub_label.text.startswith('附属信息'.decode('utf-8')):
            #    props['feature'] = sub_div.text.strip()

            if sub_label.text.startswith('网站地址'.decode('utf-8')):
                props['url'] = sub_div.text.strip()

            if sub_label.text.startswith('店铺标签'.decode('utf-8')):
                tags = [a.text.strip() for a in sub_div.findAll('a')]
                props['tags'] = tags

        detail_intro_div = div.find('div',{'class':'detail-intro'})
        if detail_intro_div:
            sub_p = detail_intro_div.find('p')
            sub_div = detail_intro_div.find('div')
            if sub_div and sub_p and sub_p.text.startswith('店铺介绍'.decode('utf-8')):
                props['description'] = sub_div.text.strip()

        return props

    def handle_promote_more(self, div):
        promote = {}
        for p in div.findAll('p'):
            sub_a = p.findChild('a')
            sub_span = p.findChild('span')

            promote_name = sub_a.text
            promote_count = int(sub_span.text[1:-1])
            promote[promote_name] = promote_count
        return {'promote':promote}
    
    def handle_impress_more(self, div):
        impress = [span.text.strip() for span in div.findAll('span')]
        return {'impress': impress}

    def handle_rating(self, div):
        props = {}
        rating_b = div.findAll('b')[0]
        if rating_b: 
            match = RATING_PATTERN.match(rating_b.text)
            if match:
                props['rating'] = float(match.group(1))

        n_comments_b = div.find('b',{'class':'s-num'})
        if n_comments_b: 
            props['n_comments'] = int(n_comments_b.text)
        return props

class Command(CrawlerBaseCommand):
    option_list = CrawlerBaseCommand.option_list + [
        make_option('--pool-size', action='store', dest='pool-size',type='int',default=1),
    ]
    def execute(self, *args, **options):
        if options['start-over']:
            init_db_for_crawl(DB_CONN_URL)

        num2crawl = options['count']
        interval = options['interval']
        if options['url']:
            # run once, test mode (no writeback)
            p = StoreDetailCrawler(interval=interval, count=num2crawl, writeback=False, url=options['url'])
            p.start()
            p.join()
        else:
            # run pool (could have pool size of 1)
            pool_size = options['pool-size']
            while (len(multiprocessing.active_children())<pool_size and not completed):
                num = pool_size - len(multiprocessing.active_children())
                for i in range(num):
                    p = StoreDetailCrawler(count=num2crawl, interval=interval)
                    p.start()
                    time.sleep(2)

                while(len(multiprocessing.active_children())>=pool_size):
                    time.sleep(0.5)
                
                print 'COMPLETED: ',completed

if __name__=="__main__":
    cmd = Command()
    cmd.run_from_argv(sys.argv)
