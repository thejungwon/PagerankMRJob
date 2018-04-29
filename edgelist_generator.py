from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep
class MREdgeListGen(MRJob):
    def mapper(self, _, line):
        line=line.replace('"','')
        page, links = line.split("\t")
        page = page.split("/")[0]
        if len(links.split()):
            for link in links.split():
                yield page, link


if __name__ == '__main__':
    MREdgeListGen.run()