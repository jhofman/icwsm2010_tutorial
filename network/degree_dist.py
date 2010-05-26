#!/usr/bin/env python

from hstream import HStream
import sys

class DegreeDistribution(HStream):
    def mapper(self, record):
        # assumes input of:
        # node in_degree out_degree in_neighbors out_neighbors

        if len(record) != 5:
            sys.stderr.write("\t".join(record)+'\n')
            return
        else:
            node, in_degree, out_degree, in_neighbors, out_neighbors = record

            # output node's in-degree and count of 1 as
            # in_k, 1
            bin = 'in_' + in_degree
            self.write_output( (bin, 1) )

            # output node's out-degree and count of 1 as
            # out_k, 1
            bin = 'out_' + out_degree
            self.write_output( (bin, 1) )


    def reducer(self, key, records):
        # total number of nodes with this degree
        total = 0
        
        for record in records:
            if len(record) != 2:
                sys.stderr.write("\t".join(record)+'\n')
                return

            bin, count = record

            # increment node count
            total += int(count)

        # write result as
        # (in|out), degree, count
        direction, degree = bin.split('_')
        self.write_output( (direction, degree, total) )

if __name__ == '__main__':

    DegreeDistribution()
