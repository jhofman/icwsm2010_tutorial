#!/usr/bin/env python
#
# file: adjacency_list.py
#
# description: converts edge list to adjacency list
#
# usage: see run_toygraph
#
# author: jake hofman (gmail: jhofman)
#

import sys
sys.path.append('.')
from hstream import HStream

class AdjacencyList(HStream):
    def mapper(self, record):
        
        if len(record) != 2:
            sys.stderr.write("\t".join(record)+'\n')
            return
        else:
            source, target = record

            # write out two records for each edge,
            # the first indicating that the source points to the
            # target
            # second indiciating that the target is pointed to by the
            # source
            self.write_output((source,'>',target))
            self.write_output((target,'<',source))

    def reducer(self, key, records):
        in_neighbors=set()
        out_neighbors=set()

        for record in records:
            assert len(record) == 3

            u, dir, v = record
            
            if dir == '>':
                # if u points to v, store as an out neighbor
                out_neighbors.add(v)
            if dir == '<':
                # if u is pointed to by v, store as an in neighbor
                in_neighbors.add(v)

        # degree is number of neighbors
        in_degree=len(in_neighbors)
        out_degree=len(out_neighbors)

        # write out node, in/out degrees, in/out neighbors
        self.write_output((key,
                           in_degree,
                           out_degree,
                           " ".join(in_neighbors),
                           " ".join(out_neighbors)))


if __name__ == '__main__':

    AdjacencyList()
