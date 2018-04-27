from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol

class MRScorePreprocessing(MRJob):
    OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self, _, line):
        page,links = line.split('\t')
        if ".html" in page:
            page = page.replace("\\","").replace('"','')

            link_dict={}
            links = links.split(" ")
            node = {}
            links = list(set(links))
            clean_links = []
            for link in links:
                link = link.replace("\\","").replace('"','')
                clean_links.append(link)
            if links:
                node['links'] = clean_links
                node['length'] = len(links)
            node['rank'] = 1.0
            yield page, node


if __name__ == '__main__':
    MRScorePreprocessing.run()
