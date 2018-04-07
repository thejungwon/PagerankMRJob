from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
import os
import re

class MRLinkExtractor(MRJob):

    def mapper_init(self):
       self.in_links = False
       self.cnt =0
       self.final_line = ""
       self.first_line = ""
       self.is_firstline = True
       print("HELLO")


    def mapper(self, _, line):
        line = line.strip()

        self.final_line = line


        if self.is_firstline :
            self.first_line = line
            self.is_firstline = False

        if '.html' in line and '/articles/' in line and '<!DOCTYPE html' in line and not self.in_links:
            self.cnt+=1
            #print(self.cnt)
            self.in_links = True

        if '</html>' in line and self.in_links:
            yield 1,1
            self.in_links = False
            self.from_page = ""

            if '.html' in line and '/articles/' in line and '<!DOCTYPE html' in line and not self.in_links:
                self.in_links = True

    # def mapper_final(self):
    #     print("======First Line========")
    #     print(self.first_line)
    #     print("======Final Line========")
    #     print(self.final_line)








    def reducer(self, key,cnt):
        print("reducer")
        yield 1 ,sum(cnt)

if __name__ == '__main__':
    MRLinkExtractor.run()
