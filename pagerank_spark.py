from __future__ import print_function

import re
import sys
from operator import add
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import functions as F


from pyspark import SparkContext

def compute_weight(urls, rank):
    for url in urls:
        num_urls = len(url.split())
        for url_each in url.split():
            yield (url_each , rank / num_urls)

def parse_link(links):
    links=links.replace('"','')
    links=links.split("\t")
    page=links[0].split("/")[0]
    return page,links[1]

def count_link(links):
    links=links.replace('"','')
    links=links.split("\t")
    page=links[0].split("/")[0]
    for link in links[1].split():
        yield '',link
if __name__ == "__main__":


    sc = SparkContext()
    sqlc = SQLContext(sc)

    file = "result.txt"
    links = sc.textFile(file).map(lambda links: parse_link(links)).distinct().groupByKey().cache()


    total_link = sc.textFile(file).flatMap(lambda links: count_link(links)).distinct().groupByKey()
    T = len(set(list(total_link.collect()[0][1])+list(links.keys().collect())))


    #
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0/float(T)))
    # df = sqlc.createDataFrame(ranks, ["id", "value"])
    # my_window = Window.partitionBy().orderBy("id")
    # df = df.withColumn("prev_value", df.value)
    # df.show()
    ranks_origin = ranks
    diff_value = 0
    number_of_iter=0
    while True:
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: compute_weight(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (0.15) + 0.85/float(T))
        ranks_origin.join(ranks).map(lambda rank: abs(rank[0][1]-rank[1][1]))

        new_diff = sc.broadcast(ranks_origin.values().sum()/len(ranks_origin.collect())).value
        print("Convergence : %s" % float(new_diff))
        ranks_origin = ranks
        number_of_iter+=1
        if diff_value == new_diff:

            break
        else:
            diff_value= new_diff



        # Collects all URL ranks and dump them to console.

    rSorted = ranks.sortBy(lambda a: a[1])
    totalRank = ranks.map(lambda x : x[1]).sum()
    for (link, rank) in rSorted.collect():
        pagename = link.replace('u','\u')
        pagename = pagename.split(".")[0]
        pagename = pagename.decode('unicode_escape')
        print("%s.html has rank: %s." % (pagename , rank))
    print("Total Rank shoulbe 1 = %s" % (totalRank))
    print("total number of interation %s" % (number_of_iter))
    sc.stop()

