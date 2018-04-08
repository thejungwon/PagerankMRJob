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
       print("MAPPER")


    def mapper(self, _, line):
        line = line.strip()


        self.final_line = line
        if not self.first_line_found:
            self.first_line = line
            self.first_line_found = True
            print("START")
            print(self.first_line)

        #for the beginning of the file
        if starting_condition(line) and not self.in_links:

            self.in_links = True
            self.from_page = line[line.find("articles"):line.find(".html")+5]
            self.outgoing_pages = []

            if len(self.incomplete_pages) >0 and not starting_condition(self.first_line) and not self.incomplete_sent:
                self.incomplete_sent = True
                page = self.from_page
                page_name = four_digit_escape(page.split(".")[0])
                if self.from_page :
                    yield page_name+".htmk/"+str(len(self.incomplete_pages)), 1
                self.incomplete_pages = []


        #for the ending of the file
        if ending_condition(line):
            if self.in_links:
                page = self.from_page
                page_name = four_digit_escape(page.split(".")[0])
            else:
                self.incomplete_pages=self.outgoing_pages

            self.from_page = ""
            self.in_links = False
            self.outgoing_pages = []

            #for the case that '</html> with new line
            if starting_condition(line):

                self.in_links = True

                self.from_page = line[line.find("articles"):line.find(".html")+5]
                if len(self.incomplete_pages) >0 and not starting_condition(self.first_line) and not self.incomplete_sent:
                    self.incomplete_sent = True
                    page = self.from_page
                    page_name = four_digit_escape(page.split(".")[0])
                    yield page_name+".htmk/"+str(len(self.incomplete_pages)), 1
                    self.incomplete_pages = []




        #for the link of the file
        if link_condition(line) :
            link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
            if link.count(".")==1 and link_condition2(link):
                self.outgoing_pages.append(link)


    def mapper_final(self):
        print("END")
        print(self.final_line)
        if self.in_links:
            page = self.from_page

            page_name = four_digit_escape(page.split(".")[0])
            yield page_name+".htmm/"+str(len(self.outgoing_pages)), 1


    def reducer_init(self):
        print("REDUCING")
    def reducer(self, key, values):
        
        yield key, 1



linkExcludingArray = ['Image~', 'Template~', 'Template%7', 'User_talk', 'User~',
'Wikipedia~','Wikipedia%7','Wikipedia_talk~','Category~','Talk~','Talk%7','Template_talk%7']
linkExcludingArray2 = ["%7E", "~", "href=http"]
# linkExcludingArray = []
# linkExcludingArray2=[]

def four_digit_escape(string):
    return string
    #return ''.join(u'u%04x'%ord(char) for char in string)
def starting_condition(line):
    if '.html' in line and '<!DOCTYPE html' in line:
    # and not any(re.findall('|'.join(linkExcludingArray), line)):
        return True
    return False
def ending_condition(line):
    if '</html>' in line :
        return True
    return False
def link_condition(line):
    if '<a' in line and 'href=' in line and '.html' in line:
    # and not any(re.findall('|'.join(linkExcludingArray), line)):
        return True
    return False
def link_condition2(link):
    return True
    # if not any(re.findall('|'.join(linkExcludingArray2), link)):
    #     return True
    # return False

if __name__ == '__main__':
    MRLinkExtractor.run()
