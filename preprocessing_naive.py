from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
import sys
import re


class MRLinkExtractor(MRJob):

    def mapper_init(self):
       self.from_page = ""
       self.in_links = False
       self.outgoing_pages = []

    def mapper(self, _, line):
        line = line.strip()


        if starting_condition(line) and not self.in_links:
            self.in_links = True
            if "index.html" in line:
                self.from_page = "index.html"
            else :
                self.from_page = line[line.find("articles"):line\
                    .find(".html")+5].split("/")[-1]
            self.outgoing_pages = []


        if ending_condition(line):
            if self.in_links:
                page_name = self.from_page
                yield page_name+".html/"+str(len(self.outgoing_pages)), ' '\
                    .join(self.outgoing_pages)

            self.from_page = ""
            self.in_links = False
            self.outgoing_pages = []

        if link_condition(line) :
            link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
            self.outgoing_pages.append(link)


def replace_useless_word(string):
    string = string.replace("_"," ")
    string = string.split("~")[-1]
    string = string.split("%7E")[-1]
    return string

def four_digit_escape(string):
    string = ''.join(u'u%04x'%ord(char) for char in string)
    return string


def starting_condition(line):
    if '.html' in line and '<!DOCTYPE html' in line:
        return True
    return False
def ending_condition(line):
    if '</html>' in line :
        return True
    return False
def link_condition(line):
    if '<a' in line and 'href=' in line and '.html' in line:
        return True
    return False


if __name__ == '__main__':
    MRLinkExtractor.run()
