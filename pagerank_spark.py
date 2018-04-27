from __future__ import print_function

import re
import sys
from operator import add
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.sql.functions import when

from pyspark import SparkContext
all_urls = []
def compute_weight(urls, rank):

    num_urls = len(urls)
    if num_urls :
        for url in urls:
            yield (url, rank / num_urls)
    else :
        for url in all_urls:
            yield (url, rank / len(all_urls))

def parse_link(links):
    links=links.replace('"','')
    links=links.split("\t")
    page=links[0].split("/")[0]
    outgoings = links[1].split()

    return page,outgoings

def count_link(links):
    links=links.replace('"','')
    links=links.split("\t")
    page=links[0].split("/")[0]
    for link in links[1].split():
        yield '',link
if __name__ == "__main__":


    sc = SparkContext()
    sqlc = SQLContext(sc)
    sc.setLogLevel("ERROR")
    file = "clean_result.txt"
    # file = sys.argv[1]
    links = sc.textFile(file).map(lambda links: parse_link(links))


    ranks = links .map(lambda url_neighbors: (url_neighbors[0], 1.0))

    ranks_origin = ranks
    diff_value = float('inf')
    number_of_iter=0
    while True:
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: compute_weight(url_urls_rank[1][0], url_urls_rank[1][1]))



        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (0.85) + 0.15)
        ranks_origin.join(ranks).map(lambda rank: (rank[0][1]-rank[1][1]))

        new_diff = sc.broadcast(ranks_origin.values().sum()).value
        print("Convergence : %s" % float(new_diff))
        ranks_origin = ranks
        number_of_iter+=1
        if diff_value-new_diff<1:
            break
        else:
            diff_value= new_diff



        # Collects all URL ranks and dump them to console.

    rSorted = ranks.sortBy(lambda a: a[1])
    totalRank = ranks.map(lambda x : x[1]).sum()
    for (link, rank) in rSorted.collect()[-20:]:
        pagename = link.replace('u','\u')
        pagename = pagename.split(".")[0]
        pagename = pagename.decode('unicode_escape')
        print("%s.html has rank: %s." % (pagename , rank))
    print("Total Rank shoulbe 1 = %s" % (totalRank))
    print("total number of interation %s" % (number_of_iter))
    sc.stop()
