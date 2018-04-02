from mrjob.job import MRJob
from bs4 import BeautifulSoup
import os
import re

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        if '.html' in line and '<!DOCTYPE html' in line:
            if 'href=' in line and '.html' in line:
                soup = BeautifulSoup(line,"html.parser")
                for a in soup.find_all('a', href=True):
                    yield a['href'].split("/")[-1], 1
                    break

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordFreqCount.run()
