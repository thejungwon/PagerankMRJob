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

        #for the beginning of the file
        if starting_condition(line) and not self.in_links:

            self.in_links = True
            self.from_page = line[line.find("articles"):line.find(".html")+5].split("/")[-1]
            # self.from_page = line[line.find("articles"):line.find(".html")+5]
            self.outgoing_pages = []

            if not starting_condition(self.first_line) and not self.incomplete_sent:
                self.incomplete_sent = True
                page_name = self.from_page

                page_name=replace_useless_word(page_name)
                page_name = four_digit_escape(page_name.split(".")[0])



                if self.from_page :
                    self.incomplete_pages = set(self.incomplete_pages)
                    self.incomplete_pages = list(self.incomplete_pages)
                    yield page_name+".htmk/"+str(len(self.incomplete_pages)), ' '.join(self.incomplete_pages)
                    # yield page_name+".htmk/"+str(len(self.incomplete_pages)), 1
                self.incomplete_pages = []


        #for the ending of the file
        if ending_condition(line):
            # print(3)

            if self.in_links:

                page_name = self.from_page
                # page_name = '/'.join(page_name.split("/")[:-1])+"/"+str(self.yield_cnt)+"_"+page_name.split("/")[-1]

                page_name=replace_useless_word(page_name)
                page_name = four_digit_escape(page_name.split(".")[0])
                self.outgoing_pages = set(self.outgoing_pages)
                self.outgoing_pages = list(self.outgoing_pages)
                yield page_name+".html/"+str(len(self.outgoing_pages)), ' '.join(self.outgoing_pages)

                # yield page_name+".html/"+str(len(self.outgoing_pages)), 1
            else:
                self.incomplete_pages=self.outgoing_pages

            self.from_page = ""
            self.in_links = False
            self.outgoing_pages = []

            #for the case that '</html> with new line
            if starting_condition(line):

                self.in_links = True
                self.from_page = line[line.find("articles"):line.find(".html")+5].split("/")[-1]
                # self.from_page = line[line.find("articles"):line.find(".html")+5]




        #for the link of the file
        if link_condition(line) :
            link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
            if link_condition2(link):
                link= replace_useless_word(link)
                link = four_digit_escape(link.split(".")[0])+".html"
                self.outgoing_pages.append(link)


    def mapper_final(self):


        if self.in_links:


            page_name = self.from_page
            # page_name = '/'.join(page_name.split("/")[:-1])+"/"+str(self.yield_cnt)+"_"+page_name.split("/")[-1]
            page_name=replace_useless_word(page_name)
            page_name = four_digit_escape(page_name.split(".")[0])

            self.outgoing_pages = set(self.outgoing_pages)
            self.outgoing_pages = list(self.outgoing_pages)
            yield page_name+".htmm/"+str(len(self.outgoing_pages)), ' '.join(self.outgoing_pages)
            # yield page_name+".htmm/"+str(len(self.outgoing_pages)), 1



    # def reducer(self, key,values):
    #     yield key, 1


# linkExcludingArray = ['Image~', 'Template~', 'Template%7', 'User_talk', 'User~',
# 'Wikipedia~','Wikipedia%7','Wikipedia_talk~','Category~','Talk~','Talk%7','Template_talk%7']
# linkExcludingArray2 = ["%7E", "~", "href=http"]
linkExcludingArray = ['=======']
linkExcludingArray2=['=======']
def replace_useless_word(string):
    # useless = ['Image~', 'Template~', 'Template%7', 'User_talk', 'User~','Wikipedia~','Wikipedia%7','Wikipedia_talk~','Category~','Talk~','Talk%7','Template_talk%7']
    string = string.replace("_"," ")
    string = string.split("~")[-1]
    string = string.split("%7E")[-1]
    # for word in useless:
    #     string=string.replace(word,"")
    return string

def four_digit_escape(string):
    string = ''.join(u'u%04x'%ord(char) for char in string)
    # string = string.replace("u","\u")
    return string
    # return string

def starting_condition(line):
    if '.html' in line and '<!DOCTYPE html' in line \
    and not any(re.findall('|'.join(linkExcludingArray), line)):
        return True
    return False
def ending_condition(line):
    if '</html>' in line :
        return True
    return False
def link_condition(line):
    if '<a' in line and 'href=' in line and '.html' in line \
    and not any(re.findall('|'.join(linkExcludingArray), line)):
        return True
    return False
def link_condition2(link):
    if not any(re.findall('|'.join(linkExcludingArray2), link)):
        return True
    return False

if __name__ == '__main__':
    MRLinkExtractor.run()
