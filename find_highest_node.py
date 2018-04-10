from sys import argv
maxR,maxS='',0.0
with open(argv[1]) as fi:
    for line in fi:
        temp=line.split()
        # print(temp)
        if float(temp[2][:-1])>maxS:
			maxS=float(temp[2][:-1])
			maxR=temp[0]
encoded_symbol = str(maxR.replace('u','\u').split(".")[0]).encode('utf-8')

page= encoded_symbol.decode('unicode_escape')
print maxR
print 'Resource with maximum score = %s (score = %f)'%(page,maxS)
