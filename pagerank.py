#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep

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
        return ([MRStep(mapper=self.mapper, reducer=self.reducer)] * 1)


if __name__ == '__main__':
   	MRPageRank.run()
