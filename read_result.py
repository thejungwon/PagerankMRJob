with open('test.txt') as f:
    lines = f.readlines()
    cnt = 0
    for line in lines:
        if "htmm" in line.split('\"')[1] or "htmk" in line.split('\"')[1] :
            print line.split('\"')[1]
        cnt+=1

print "Count : %d" %(cnt)
