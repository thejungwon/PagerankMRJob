import sys
from operator import add

from pyspark import SparkContext
def compute_weight(page,urls, rank):
    yield (page, 0)
    for url in urls: yield (url, rank / len(urls))

def parse_link(links):
    links=links.replace('"','')
    links=links.split("\t")
    page=links[0].split("/")[0]
    outgoings = links[1].split()

    return page,outgoings

if __name__ == "__main__":


    sc = SparkContext()
    sc.setLogLevel("ERROR")
    # file = "cleaner_result.txt"
    file = sys.argv[1]
    links = sc.textFile(file).map(lambda links: parse_link(links)).cache()
    ranks = links.map(lambda (page, links): (page, 1.0))
    ranks_previous= ranks
    number_of_iter=0
    while True:
        alllinks = links.join(ranks).flatMap(
            lambda (page,(links, rank)): compute_weight(page,links,rank))

        ranks = alllinks.reduceByKey(add).mapValues(lambda rank: rank*0.85+0.15)
        convergence= ranks_previous.join(ranks).map(lambda rank: abs(rank[1][0]-rank[1][1])).sum()
        print("Convergence : %s" % float(convergence))
        ranks_previous= ranks
        number_of_iter+=1
        if convergence<1:
            break

    ranks= ranks.sortBy(lambda a: a[1])
    totalRank = ranks.map(lambda x : x[1]).sum()
    for (link, rank) in ranks.collect()[-20:]:
        pagename = link.replace('u','\u')
        pagename = pagename.split(".")[0]
        pagename = pagename.decode('unicode_escape')
        print("%s.html has rank: %s." % (pagename , rank))
    print("Total Rank shoul be the same= %s" % (totalRank))
    print("total number of interation %s" % (number_of_iter))
    sc.stop()
