from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol

class MRScorePreprocessing(MRJob):
    OUTPUT_PROTOCOL = JSONProtocol
    def configure_args(self):
        super(MRScorePreprocessing, self).configure_args()

        self.add_passthru_arg('--num-of-page', dest="totalnumber", type=int, default=10000, help='please type the number of page')


    def mapper(self, _, line):

        page,links = line.split('\t')
        if ".html" in page:
            page = page.split("/")[0]
            page = page.replace("\\","")
            page = page.replace('\"',"")

            link_dict={}
            links = links.split(" ")
            node = {}


            set_links = set(links)
            links = list(set_links)
            clean_links = []
            for link in links:
                link = link.replace('"','')
                link = link.replace("\\","")
                clean_links.append(link)

            if links:
                node['links'] = clean_links
                node['length'] = len(links)

            node['rank'] = 1.0/self.options.totalnumber

            yield page, node


if __name__ == '__main__':
    MRScorePreprocessing.run()
