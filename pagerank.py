#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep
from datetime import datetime
import sys


class MRPageRank(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    def mapper(self, page_name, page):
        L = page.get("length")
        if L :
            yield page_name, ('page', page)
            for to_page in page.get('links'):
                yield to_page, ('rank', page['rank'] / L)

    def reducer(self, page_name, values):
        page = {}
        total_score = 0
        for which_type, value in values:
            if which_type == 'page':
                page = value
            elif which_type == 'rank':
                total_score += value
        d = 0.85
        if page :
            page['rank'] = 1 - d + d * total_score
            yield page_name, page
        else :
            print(page_name)
    def summary_mapper(self, page_name, page):
        yield '', page.get('rank')
    def summary_reducer(self, page_name, values):
        yield '', sum(values)
    def steps(self):
        # return ([MRStep(mapper=self.mapper, reducer=self.reducer)] * 10+[MRStep(mapper=self.summary_mapper,reducer=self.summary_reducer)])
        return ([MRStep(mapper=self.mapper, reducer=self.reducer)] * 16)


if __name__ == '__main__':
    start_time = datetime.now()
    MRPageRank.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    sys.stderr.write(str(elapsed_time)+"\n")
