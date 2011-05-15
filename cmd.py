#!/usr/bin/python
# encoding=utf8

import sys
from optparse import OptionParser, make_option 
class BaseCommand(object):
    '''
    A basic command class that handles
        -v, --verbose
        -h, --help
    '''
    option_list = [
        make_option('-v','--verbose', action='store_true', dest='verbose', default=False,
                   help='turn on verbose mode'),
    ]

    verbose = False

    def __init__(self):
        super(BaseCommand, self).__init__()
    
    def print_help(self):
        self.parser.print_help()

    def create_parser(self, prog_name):
        '''
        Create and return the ``OptionParser`` which will be used to parse the arguments
        to this command
        '''
        return OptionParser(prog=prog_name,
                            option_list=self.option_list)
   
    def run_from_argv(self, argv):
        ''' Parse options and call self.execute '''
        self.parser = self.create_parser(argv[0])
        options, args = self.parser.parse_args(argv[1:])
        self.verbose = options.verbose
        self.execute(*args, **options.__dict__)

    def execute(self, *args, **options):

        raise NotImplementedError()

class CrawlerBaseCommand(BaseCommand):
    option_list = BaseCommand.option_list + [
        make_option('--start-over', action='store_true', dest='start-over', default=False,
                   help='recreate the whole database to crawler and start over'),

        make_option('--url', action='store', dest='url', default=None,
                    help='test mode: run crawler only on this link and print out result'),

        make_option('--count', action='store', type='int', dest='count', default=10,
                    help='number of links the crawler will try to crawl'),

        make_option('--interval', action='store', type='int', dest='interval', default=0,
                    help='crawling frequency control, 1 link will be crawled in at least ``interval`` seconds'),
    ]

class DatabaseBaseCommand(BaseCommand):
    option_list = BaseCommand.option_list + [
        make_option('--syncdb',action='store_true',dest='syncdb',default=False,
                   help='create corresponding dababase schema that defined in model.py'),

        make_option('--init-collect', action='store_true', dest='init-collect', default=False,
                    help='initiate database for collecting link2list, initial seed will be injected into database'),

        make_option('--init-crawl', action='store_true', dest='init-crawl', default=False,
                    help='initiate database for crawling link2detail'),

        make_option('--dump', action='store', dest='dumpfile', default=None,
                    help='dump the whole database to a file named `DUMP`'),
    ]
