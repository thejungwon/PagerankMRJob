from mrjob.job import MRJob

import os
import re
import json
class MRLinkExtractor(MRJob):

    def mapper_init(self):
       self.from_page = ""
       self.in_links = False
       self.outgoing_pages = []

    def mapper(self, _, line):
        line = line.strip()
        if '.html' in line and '/articles/' in line and '<!DOCTYPE html' in line and not self.in_links:
            self.in_links = True
            self.from_page = "FROM:"+line[line.find("articles"):line.find(".html")+5].split("/")[-1]


        if '</html>' in line and self.in_links:
            yield self.from_page+"/"+str(len(self.outgoing_pages)), ' '.join(self.outgoing_pages)
            self.from_page = ""
            self.in_links = False
            self.outgoing_pages = []

        if 'href=' in line and '.html' in line and self.in_links:
            link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
            self.outgoing_pages.append(link)


if __name__ == '__main__':
    MRLinkExtractor.run()
