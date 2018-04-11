from sys import argv
import json
import pprint
page,max='',0.0
with open(argv[1]) as fi:
    for line in fi:
        temp=line.split("\t")
        rank = json.loads(temp[-1])['rank']
        if float(rank)>max:
			max=float(rank)
			page=temp[0].replace('"','')
encoded_symbol = str(page.replace('u','\u').split(".")[0]).encode('utf-8')
print 'unicode version %s' %(encoded_symbol)
page= encoded_symbol.decode('unicode_escape')
print 'utf-8 version %s'%(page)
print 'The most valuable page is %s.html (rank = %f)'%(page,max)
