#!/usr/bin/env python

from hstream import HStream
import sys

class AdjacencyList(HStream):    
    def mapper(self, record):
        # assumes input:
        # source node<tab>target node
        if len(record) != 2:
            sys.stderr.write("\t".join(record)+'\n')
            return
        else:
            u, v = record

            # write out 
            self.write_output((u,'>',v))
            self.write_output((v,'<',u))

    def reducer(self, key, records):
        in_neighbors=set()
        out_neighbors=set()

        for record in records:
            assert len(record) == 3

            u, dir, v = record
            
            if dir == '>':
                out_neighbors.add(v)
            if dir == '<':
                in_neighbors.add(v)


        in_degree=len(in_neighbors)
        out_degree=len(out_neighbors)

        self.write_output((key,
                           in_degree,
                           out_degree,
                           " ".join(in_neighbors),
                           " ".join(out_neighbors)))


if __name__ == '__main__':

    AdjacencyList()
