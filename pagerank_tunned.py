#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep
from datetime import datetime
import sys


class MRPageRank(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    def configure_args(self):
        super(MRPageRank, self).configure_args()
        self.add_passthru_arg('--num-of-page', dest="totalnumber", type=int, default=10000, help='please type the number of page')

    def mapper(self, page_name, page):

        if 'links' in page:
            yield page_name, ('page', page)
            L = page.get("length")
            for to_page in page.get('links'):
                if L :
                    yield to_page, ('rank', page['rank'] / L)

    def combiner(self, page_name, values):
        page = {}
        partial_score = 0.0
        pageExist = False
        linkExist = False
        link_list = ()
        for which_type, value in values:
            # print("In for")
            if which_type == 'page':
                page = value
                pageExist = True
                # yield page_name, ('page', page)
            elif which_type == 'rank':
                partial_score += value
                linkExist= True
                # link_list+=('rank', value)
                # yield page_name, ('rank', value)

        if pageExist :
            yield page_name, ('page', page)
        if linkExist :
            yield page_name, ('rank', partial_score)



    def reducer(self, page_name, values):

        page = {}
        total_score = 0

        for which_type, value in values:
            # print("In for")
            if which_type == 'page':
                page = value
                # print(page_name)
            elif which_type == 'rank':
                total_score += value

        d = 0.5
        T=self.options.totalnumber
        # print(page_name)
        # print("====================================")

        page['rank'] = d/T +(1 - d) * total_score

        yield page_name, page


    def steps(self):
        return ([MRStep(mapper=self.mapper,combiner=self.combiner, reducer=self.reducer)] * 3)


if __name__ == '__main__':
    start_time = datetime.now()
    MRPageRank.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    sys.stderr.write(str(elapsed_time)+"\n")
