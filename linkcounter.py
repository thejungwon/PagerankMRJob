from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
import os
import re
import json
class MRLinkExtractor(MRJob):

    def mapper_init(self):
       self.from_page = ""
       self.in_links = False
       self.outgoing_pages = []
       self.pageCnt = 0


    def mapper(self, _, line):
        line = line.strip()
        if '.html' in line and '/articles/' in line and '<!DOCTYPE html' in line and not self.in_links:
            self.pageCnt+=1
            self.in_links = True
            self.from_page = line[line.find("articles"):line.find(".html")+5].split("/")[-1]
            self.outgoing_pages = []


        if '</html>' in line and self.in_links:
            yield 1,len(self.outgoing_pages)
            self.from_page = ""
            self.in_links = False
            self.outgoing_pages = []

        if 'href=' in line and '.html' in line and self.in_links :
            link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
            if link.count(".")==1:
                print link
                self.outgoing_pages.append(link)

    def reducer(self, page,cnt):
        yield page ,sum(cnt)

if __name__ == '__main__':
    MRLinkExtractor.run()
