#!/usr/bin/python
import sys

from crawler.daodao.crawl import Command as crawl_cmd
from crawler.daodao.collect import Command as collect_cmd
from crawler.daodao.db import Command as db_cmd


cmd_dict = {
    'crawl': crawl_cmd,
    'collect': collect_cmd,
    'db': db_cmd,
}

def usage():
    print """
    USAGE: python manage.py [subcommand] [options]

    SUBCOMMAND:
        crawl      crawler module for attraction detail page
        collect    link-collect module for attraction list page
        db         database module

    Get more detailed help via:
        python manage.py [subcommand] -h 
        python manage.py [subcommand] --help
    """
if __name__=='__main__':
    if sys.argv[1] in ('-h','--help'):
        usage()
        
    cmd = cmd_dict[sys.argv[1]]()
    argv = [sys.argv[0]]
    argv.extend(sys.argv[2:])
    cmd.run_from_argv(argv)
