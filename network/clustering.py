#!/usr/bin/env python

from hstream import HStream
from collections import defaultdict
import sys
from datetime import datetime

class ClusteringCoefficient(HStream):
    def mapper(self, record):
        # assumes input of:
        # node in_degree out_degree in_neighbors out_neighbors

        if len(record) != 5:
            sys.stderr.write("\t".join(record)+'\n')
            return
        else:
            u, in_degree, out_degree, in_neighbors, out_neighbors = record

            # note: for completed inbound triangles:
            # in_neighbors, out_neighbors = out_neighbors, in_neighbors

            # for each in_neighbor v write:
            # v u out_neighbors
            [self.write_output((v,u,out_neighbors)) \
             for v in in_neighbors.split()]


    def reducer(self, key, records):
        # assumes input
        # node one_hop_neighbor two_hop_neighbors
        
        # dictionaries to store one and two hop neighbors
        # note: two hop includes one hop neighbors
        onehop={}
        twohop=defaultdict(int)

        for record in records:
            if len(record) == 2:
                # no out neighbors
                # just record v as one hop neighbor
                u, v = record
                onehop[v]=1

            else:
                assert len(record) == 3
                u, v, ws  = record

                # record one and two hop neighbors
                onehop[v]=1
                for w in ws.split():
                    twohop[w]+=1

        # node degree
        size_onehop=len(onehop)
        # number of nodes within two hops
        size_twohop=size_onehop+len(twohop)

        if size_onehop > 20000:
            sys.stderr.write('[%s] node %s: onehop %d, one+twohop: %d\n' %
                             (datetime.now(), key, size_onehop, size_twohop))

        # find intersection of nodes in one and two hop neighborhoods
        # sum up number of paths to second hop node
        onetwo=[twohop[k] for k in onehop.keys() if k in twohop]
        triangles=0.5*sum(onetwo)

        # number of nodes exactly two hops away
        size_twohop-=len(onetwo)
        
        self.write_output((key, triangles, size_onehop, size_twohop))

        if size_onehop > 20000:
            sys.stderr.write('[%s] node %s: done\n' %
                             (datetime.now(), key))

if __name__ == '__main__':

    ClusteringCoefficient()
