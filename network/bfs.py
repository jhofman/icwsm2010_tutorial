#!/usr/bin/env python
#
# file: bfs.py
#
# description: runs one round of breadth-first search
#
# usage: see run_toygraph
#
# author: jake hofman (gmail: jhofman)
#

import sys
sys.path.append('.')
from hstream import HStream
from collections import defaultdict

class BreadthFirstSearch(HStream):

    def mapper(self, record):
        if len(record) != 3:
            sys.stderr.write("\t".join(record)+'\n')
            return

        node, distance, neighbors = record

        # output original record
        self.write_output(record)

        # use float for distance to accomodate 'inf'
        distance = float(distance)

        # if node is reachable, neighbors are distance+1 away
        if distance < float('inf') and neighbors:

            # output each neighbor and distance+1
            for neighbor in neighbors.split(' '):
                self.write_output( (neighbor, int(distance+1)) )


    def reducer(self, key, records):

        # initialize minimum distance
        min_distance = float('inf')

        for record in records:

            if len(record) == 3:
                # original record
                node, distance, neighbors = record

            elif len(record) == 2:
                # updated distance
                node, distance = record

            else:
                sys.stderr.write("\t".join(record)+'\n')
                return

            # update minimum distance
            min_distance = min(min_distance, float(distance))

        # convert to int or leave as 'inf')
        if min_distance < float('inf'):
            min_distance = int(min_distance)
            
        self.write_output( (node, min_distance, neighbors) )
                    
if __name__=='__main__':
    BreadthFirstSearch()
                        

