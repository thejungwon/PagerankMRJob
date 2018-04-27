from sys import argv
import json
import pprint
page,max='',0.0
page_dict={}
with open(argv[1]) as fi:
    for line in fi:
        temp=line.split("\t")
        rank = json.loads(temp[-1])['rank']
        page=temp[0].replace('"','')
        page_dict[page]=float(rank)
for page in sorted(page_dict, key=page_dict.get, reverse=True)[0:20]:

    encoded_symbol = str(page.replace('u','\u').split(".")[0]).encode('utf-8')
    # print 'unicode version %s' %(encoded_symbol)
    rank = page_dict[page]
    page= encoded_symbol.decode('unicode_escape')
    # print 'utf-8 version %s'%(page)
    print 'PageRank is %s.html (rank = %f)'%(page,rank )
