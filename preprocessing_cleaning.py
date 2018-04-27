from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep
class MRLinkCleaning(MRJob):

    def mapper(self, _, line):
        line=line.replace('"','')
        page, links = line.split("\t")
        page = page.split("/")[0]
        yield page, ('page','')


        for link in links.split():
            yield link, ('link',page)

    def reducer(self, page, values):
        page_exist = False
        from_pages = []

        for type, value in values:
            if type == "page":
                page_exist=True
            else :
                from_pages.append(value)

        if page_exist :
            for link in from_pages:
                yield link, page

    def reverse(self, page, values):
        yield page, ' '.join(values)


    def steps(self):
        return ([MRStep(mapper=self.mapper, reducer=self.reducer),MRStep(reducer=self.reverse)])


if __name__ == '__main__':
    MRLinkCleaning.run()
