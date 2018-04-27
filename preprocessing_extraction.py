from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
import sys
import re


class MRLinkExtractor(MRJob):

    def mapper_init(self):
       self.from_page = ""
       self.in_links = False
       self.outgoing_pages = []
       self.first_line = ""
       self.final_line = ""
       self.first_line_found =False
       self.incomplete_pages=[]
       self.incomplete_from_page = ""
       self.incomplete_sent = False

    def mapper(self, _, line):
        line = line.strip()
        self.final_line = line
        if not self.first_line_found:
            self.first_line = line
            self.first_line_found = True

        if starting_condition(line) and not self.in_links:
            self.in_links = True
            if "index.html" in line:
                self.from_page = "index.html"
            else :
                self.from_page = line[line.find("articles"):line.find(".html")+5].split("/")[-1]
            self.outgoing_pages = []
            if not starting_condition(self.first_line) and not self.incomplete_sent:
                self.incomplete_sent = True
                page_name = four_digit_escape(replace_useless_word(self.from_page).split(".")[0])

                if self.from_page :
                    self.incomplete_pages = list(set(self.incomplete_pages))
                    yield page_name+".htmk", ' '.join(self.incomplete_pages)
                self.incomplete_pages = []

        #for the ending of the file
        if ending_condition(line):
            if self.in_links:
                page_name = self.from_page
                page_name = replace_useless_word(page_name)
                page_name = four_digit_escape(page_name.split(".")[0])
                self.outgoing_pages = list(set(self.outgoing_pages))
                yield page_name+".html", ' '.join(self.outgoing_pages)
            else:
                self.incomplete_pages = self.outgoing_pages

            self.from_page = ""
            self.in_links = False
            self.outgoing_pages = []

            if starting_condition(line):
                self.in_links = True
                self.from_page = line[line.find("articles"):line.find(".html")+5].split("/")[-1]

        if link_condition(line) :
            link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
            link = replace_useless_word(link)
            link = four_digit_escape(link.split(".")[0])+".html"
            self.outgoing_pages.append(link)


    def mapper_final(self):

        if self.in_links:
            page_name = self.from_page
            page_name = replace_useless_word(page_name)
            page_name = four_digit_escape(page_name.split(".")[0])
            self.outgoing_pages = list(set(self.outgoing_pages))
            yield page_name+".htmm", ' '.join(self.outgoing_pages)


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
