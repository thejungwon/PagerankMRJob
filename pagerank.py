#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep

class MRPageRank(MRJob):

    INPUT_PROTOCOL = JSONProtocol

    def mapper(self, page_name, page):


        if 'links' in page:
            yield page_name, ('page', page)
            for to_page, value in page.get('links'):
                yield to_page, ('rank', page['rank'] * value)

    def reducer(self, page_name, values):

        page = {}
        total_score = 0

        for which_type, value in values:
            if which_type == 'page':
                page = value
            elif which_type == 'rank':
                total_score += value

        d = 0.7

        page['rank'] = 1 - d + d * total_score

        yield page_name, page


    def steps(self):
        return ([MRStep(mapper=self.mapper, reducer=self.reducer)] * 3)


if __name__ == '__main__':
   	MRPageRank.run()
