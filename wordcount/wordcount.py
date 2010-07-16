#!/usr/bin/env python
#
# file: wordcount.py
#
# description: the obligatory wordcount example for an introduction to
# mapreduce, implemented in hadoop streaming using a simple wrapper
# class (hstream). counts the number of times each word in the input
# occurs.
#
# usage:
#   locally:
#     map only: cat input.txt | ./wordcount.py -m
#     map+"shuffle": cat input.txt | ./wordcount.py -m | sort -k1
#     map+"shuffle"+reduce: cat input.txt | ./wordcount.py -r
#       which is equivalent to:
#       cat input.txt | ./wordcount.py -m | sort -k1 | ./wordcount.py -r
#   distributed:
#     (assumes $HADOOP_HOME is set to your hadoop install)
#
#     $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*-streaming.jar \
#       -input input.txt \
#       -output wc_output \
#       -mapper 'wordcount.py -m' \
#       -reducer 'wordcount.py -r' \
#       -file wordcount.py \
#       -file hstream.py
#
# author: jake hofman (gmail: jhofman)
#

# import simple hadoop streaming class to simply definition of mapper
# and reducer functions
import sys
sys.path.append('.')
from hstream import HStream


class WordCount(HStream):
    """
    hadoop streaming class to count the number of times each word in
    the input occurs.
    """
    
    def mapper(self, record):
        """
        the wordcount mapper, which splits each line into words and
        produces an intermediate key (the word) and value (count of 1)
        for each word occurence on the line.
        """
        
        # join all words on the the line into one string
        # and split on whitespace, producing a tuple of words
        #
        # note: record is a tuple, automatically split on the default
        # delimiter (tab)
        words = " ".join(record).split()

        # loop over each word, writing the word (as key) and count of
        # 1 (as value)
        for word in words:
            self.write_output((word, 1))


    def reducer(self, key, records):
        """
        the wordcount reducer, which receives all intermediate records
        for a given word (the key) and adds the corresponding counts
        (the values).
        """

        # total counts for this word
        total = 0

        # loop over records, adding counts to total
        for record in records:
            # extract the fields from the tuple
            word, count = record

            # note: record is a tuple of strings, so explicitly cast
            # to an int here
            total += int(count)

        self.write_output( (word, total) )


if __name__ == '__main__':
    # call the class
    #
    # this reads command line arguments from sys.argv to check for
    # flag indicating which function(s) to perform.
    # 
    # i.e. "-m runs the mapper, -r the reducer, and -l runs the
    # mapper, followed by sort, followed by the reducer"
    WordCount()
    
