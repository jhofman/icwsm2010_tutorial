#!/usr/bin/env python
#
# file: hstream.py
#
# description: simple class for implementing hadoop streaming jobs in python
#   adapted from: 
#     http://www.michael-noll.com/wiki/Writing_An_Hadoop_MapReduce_Program_In_Python
#
# usage: inherit the HStream class and define the mapper and reducer
# functions, e.g. in myjob.py:
#
#   from hstream import HStream
#
#   class MyJob(Hstream):
#     def mapper(self, record):
#       # ...
#       self.write_output( (key, value) )
#
#     def reducer(self, key, records):
#       # ...
#       for record in records:
#           # ...
#       self.write_output( (key, value) )
#
#   if __name__=='__main__':
#     MyJob()
#
# to run mapper: ./myjob.py -m < input
# to run reducer: ./myjob.py -m < input
# to run full job: ./myjob.py -l < input
#
# to specify additional command line arguments, append arg=val after
# the -[m|r|l] switch, e.g. ./myjob.py -m max=10. you can then
# retrieve these arguments using self.args,
# e.g. self.args['max']. this is often useful for loading a parameter
# when initializing the mapper or reducer, e.g. in mapper_init,
# self.max = self.args['max'], which will then be acessible as
# self.max in mapper().
# 
# see examples for more details.
#
# author: jake hofman (hofman@yahoo-inc.com)
#

from itertools import groupby
from operator import itemgetter
import sys
from optparse import OptionParser
from StringIO import StringIO


class HStream:
    """
    simple wrapper class to facilitate writing hadoop streaming jobs
    in python. inherit the class and define mapper and reducer functions.
    see header of hstream.py for more details.
    """
    default_delim='\t'
    default_istream=sys.stdin
    default_ostream=sys.stdout
    default_estream=sys.stderr
    
    def __init__(self,
                 delim=default_delim,
                 istream=default_istream,
                 ostream=default_ostream):
        self.delim=delim
        self.istream=istream
        self.ostream=ostream

        self.parse_args()
        
    def read_input(self): 
        for line in self.istream:
            yield line.rstrip('\n').split(self.delim)

    def write_output(self,s):
        if type(s) is str:
            self.ostream.write(s + '\n')
        else:
            self.ostream.write(self.delim.join(map(str,s)) + '\n')

    def map(self):
        self.mapper_init()

        for record in self.read_input():
            self.mapper(record)

        self.mapper_end()

    def reduce(self):
        self.reducer_init()

        data = self.read_input()
        for key, records in groupby(data, itemgetter(0)):
            self.reducer(key, records)

        self.reducer_end()

    def combine(self):
        data = self.read_input()
        self.combiner( data )

    def mapper_init(self):
        return

    def mapper(self, record):
        self.write_output(record[0])

    def mapper_end(self):
        return

    def reducer_init(self):
        return

    def reducer(self, key, records):
        for record in records:
            self.write_output(self.delim.join(record))

    def reducer_end(self):
        return

    def combiner(self, records):
        for record in records:
            self.write_output(self.delim.join(record))

    def parse_args(self):
        
        parser=OptionParser()
        parser.add_option("-m","--map",
                          help="run mapper",
                          action="store_true",
                          dest="run_map",
                          default="False")
        parser.add_option("-r","--reduce",
                          help="run reduce",
                          action="store_true",
                          dest="run_reduce",
                          default="False")
        parser.add_option("-c","--combine",
                          help="run combiner",
                          action="store_true",
                          dest="run_combine",
                          default="False")
        parser.add_option("-l","--local",
                          help="run local test of map | sort | reduce",
                          action="store_true",
                          dest="run_local",
                          default="False")

        opts, args = parser.parse_args()

        self.args=dict([s.split('=',1) for s in args])

        if opts.run_map is True:
            self.map()
        elif opts.run_reduce is True:
            self.reduce()
        elif opts.run_combine is True:
            self.combine()
        elif opts.run_local is True:
            self.run_local()
            

    def run_local(self):
        map_output=StringIO()

        # map stdin to temporary string stream
        self.istream=sys.stdin
        self.ostream=map_output
        self.map()

        # sort string stream
        map_output.seek(0)        
        reduce_input=StringIO(''.join(sorted(map_output)))

        # reduce string stream to stdout
        self.istream=reduce_input
        self.ostream=sys.stdout
        self.reduce()

        
if __name__=='__main__':
    
    pass
