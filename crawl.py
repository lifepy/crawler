#!/usr/bin/python
#encoding utf-8
import sys
import time
import multiprocessing
class DetailBaseCrawler(multiprocessing.Process):
    """
    Given a list of urls, DetailCrawler will try to collect information based on specific
    page format detail page. It then stores these detailed information into database
    """
    test_mode = False
    def __init__(self, count=1, interval=0, writeback=True, url=None, *args, **kwargs):
        super(DetailBaseCrawler, self).__init__(*args, **kwargs)
        assert type(count) is int
        assert count > 0
        assert type(interval) is int

        self.count = count
        self.interval = interval
        self.writeback = writeback
        self.__db__ = self.get_db_session()

        print '-'*80
        print 'Crawler START UP'
        # if `url` is set, run in test mode
        if type(url) is str:
            self.name = 'TEST'
            self.url = url
            self.test_mode = True
        else:
            self.links = self.get_links()

            # if no more links to follow, exit
            if self.links is None or self.links == []:
                global completed
                completed = True
                sys.exit(0)

            # mark for 'SCRAPING'
            for link in self.links:
                link.status = 'SCRAPING'
                print "Target: %s" %(link.name)

            if self.writeback:
                self.__db__.begin()
                for link in self.links:
                    self.__db__.merge(link)
                self.__db__.commit()

            self.link_obj = self.links[0]
            self.name = self.links[0].name.decode('utf-8')
            self.url = self.links[0].url

    def fetch(self, url):
        '''
        Return a unicode string of corresponding url. Database will be checked beforehand
        to see if the page has already been crawled, if so, no actual crawling happens, 
        otherwise, the crawled page will be saved into database.
        '''
        content = self.fetch_from_db(url)
        if content is None:
            try:
                content = self.fetch_from_web(url)
            except Exception as e:
                print e
                print 'error happened'

            self.save_page(url, content)
        return content

    def encode(self, props, encoding='utf-8'):
        '''Given a dictionary `props`, encode every unicode to `encoding`'''
        def encode_str(item, encoding='utf-8'):
            if type(item) is unicode:
                return item.encode(encoding)
            else:
                return item
        p = {}
        for key, val in props.items():
            p[key] = encode_str(val)
            if type(val) is list:
                p[key] = [encode_str(item) for item in val]
            elif type(val) is dict:
                d = {}
                for k, v in val.items():
                    k = encode_str(k)
                    v = encode_str(v)
                    d[k] = v
                p[key] = d
        return p                

    def run(self,  *args, **kwargs):
        if self.test_mode:
            self.content = self.fetch(self.url)
            self.props = self.parse(self.content)
            for k,v in self.props.items():
                if type(v) is dict:
                    print k,"=>"
                    for kk,vv in v.items():
                        print '\t',kk, '>', vv
                elif type(v) is list:
                    print k,'=>'
                    for vv in v:
                        print '\t',vv
                else:
                    print k,'=>',v
            self.save(self.props)
            sys.exit(0)

        for link_obj in self.links:
            if self.exist_link(link_obj.url):
                continue
            start = time.time()
            self.link_obj = link_obj
            self.url = link_obj.url
            self.name = link_obj.name.decode('utf-8')
            try:
                self.content = self.fetch(self.url)
                self.props = self.parse(self.content)
                # save result into database
                if self.writeback:
                    self.save(self.props)
            except KeyboardInterrupt:
                link_obj.status = 'NEW'
                self.__db__.merge(link_obj)
            except:
                link_obj.status = 'ERROR'
                self.__db__.merge(link_obj)
                raise
            finally:
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

    def save_page(self, url, content):
        raise NotImplementedError('save_page not implemented!')

    def get_db_session(self):
        raise NotImplementedError('get_db_session not implemented!')
    
    def get_links(self):
        raise NotImplementedError('get_links not implemented')

    def fetch_from_db(self, url):
        return NotImplementedError('fetch_from_db not implemented!')

    def fetch_from_web(self, url):
        return NotImplementedError('fetch_from_web not implemented!')

    def parse(self, content):
        '''Given a unicode string of page, parse it into a dictionary'''
        raise NotImplementedError('parse not implemented!')

    def save(self, props):
        raise NotImplementedError('save is not impelmented')
