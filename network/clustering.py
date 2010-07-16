#!/usr/bin/env python
#
# file: clustering.py
#
# description: calculates number of directed triangles each node is a
# member of, as well as size of one/two-hop neighborhoods.
#
# usage: see run_toygraph
#
# author: jake hofman (gmail: jhofman)
#

import sys
sys.path.append('.')
from hstream import HStream
from collections import defaultdict

class ClusteringCoefficient(HStream):
    def mapper(self, record):

        if len(record) != 5:
            sys.stderr.write("\t".join(record)+'\n')
            return
        else:
            u, in_degree, out_degree, in_neighbors, out_neighbors = record

            # note: for completed inbound triangles, swap in and out neighbors
            # in_neighbors, out_neighbors = out_neighbors, in_neighbors

            # pass all out_neighbors to each in_neighbor
            # to compile directed two-hop neighborhood
            #
            # read as "v goes through u to reach out_neighbors"
            [self.write_output((v,u,out_neighbors)) \
             for v in in_neighbors.split()]


    def reducer(self, key, records):        
        # dictionaries to store one and two hop neighbors
        # note: two hop includes one hop neighbors
        onehop={}
        twohop=defaultdict(int)

        for record in records:
            if len(record) == 2:
                # no out-neighbors
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

        # find intersection of nodes in one and two hop neighborhoods
        # sum up number of paths to second hop node
        onetwo=[twohop[k] for k in onehop.keys() if k in twohop]
        triangles=0.5*sum(onetwo)

        # number of nodes exactly two hops away
        size_twohop-=len(onetwo)
        
        self.write_output((key, triangles, size_onehop, size_twohop))

if __name__ == '__main__':

    ClusteringCoefficient()
