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
                            usage=self.usage,
                            option_list=self.option_list)
   
    def run_from_argv(self, argv):
        ''' Parse options and call self.execute '''
        self.parser = self.create_parser(argv[0])
        options, args = self.parser.parse_args(argv[1:])
        self.verbose = options.verbose
        self.execute(*args, **options.__dict__)

    def execute(self, *args, **options):

        raise NotImplementedError()
