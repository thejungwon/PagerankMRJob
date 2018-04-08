from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
import sys


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


    def mapper(self, _, line):
        line = line.strip()

        self.final_line = line
        if not self.first_line_found:
            self.first_line = line
            self.first_line_found = True

        #for the beginning of the file
        if starting_condition(line) and not self.in_links:
            self.in_links = True
            self.from_page = line[line.find("articles"):line.find(".html")+5].split("/")[-1]
            self.outgoing_pages = []
            if len(self.incomplete_pages) >0 :

                page = self.from_page
                page_name = four_digit_escape(page.split(".")[0])

                yield page_name+".htmk/"+str(len(self.incomplete_pages)), ' '.join(self.incomplete_pages)
                self.incomplete_pages = []


        #for the ending of the file
        if ending_condition(line):
            if self.in_links:
                page = self.from_page
                page_name = four_digit_escape(page.split(".")[0])
                yield page_name+".html/"+str(len(self.outgoing_pages)), ' '.join(self.outgoing_pages)
                self.from_page = ""
                self.in_links = False
                self.outgoing_pages = []
            else :
                self.incomplete_pages=self.outgoing_pages
            #for the case that '</html> with new line
            if starting_condition(line):
                self.in_links = True
                self.from_page = line[line.find("articles"):line.find(".html")+5].split("/")[-1]


        #for the link of the file
        if link_condition(line):
            link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
            if link.count(".")==1:
                self.outgoing_pages.append(link)


    def mapper_final(self):
        if not ending_condition(self.final_line) and self.in_links:
            page = self.from_page
            page_name = four_digit_escape(page.split(".")[0])
            # print(self.outgoing_pages)
            yield page_name+".htmm/"+str(len(self.outgoing_pages)), ' '.join(self.outgoing_pages)


def four_digit_escape(string):
    return string
    #return ''.join(u'u%04x'%ord(char) for char in string)
def starting_condition(line):
    if '.html' in line and '<!DOCTYPE html' in line \
    and not 'Image~' in line and not 'Template~' in line and not 'User_talk' in line and not 'User~' in line\
    and not 'Wikipedia~' in line and not 'Wikipedia_talk~' in line and not 'Category~' in line and not 'Talk~' in line:
        return True
    return False
def ending_condition(line):
    if '</html>' in line :
        return True
    return False
def link_condition(line):
    if 'href=' in line and '.html' in line \
    and not 'Image~' in line and not 'Template~' in line and not 'User_talk' in line and not 'User~' in line\
    and not 'Wikipedia~' in line and not 'Wikipedia_talk~' in line and not 'Category~' in line and not 'Talk~' in line:
        return True
    return False

if __name__ == '__main__':
    MRLinkExtractor.run()
