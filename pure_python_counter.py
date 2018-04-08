import tarfile,os
import bs4
from sys import argv
tar = tarfile.open(argv[1],'r')
page_cnt = 0
link_cnt =0
for member in tar.getmembers():
    f=tar.extractfile(member)
    content=f.read()

    if '.html' in member.name and '/articles/' in member.name :

        page_cnt+=1
        print (member.name)
        for line in content.split("\n"):
            if '<a href=' in line and '.html' in line:
                link =  line[line.find("href=")+6:line.find(".html")+5].split("/")[-1]
                if link.count(".")==1:
                    #print(link)
                    link_cnt+=1

tar.close()

print "Total Page : %d " %(page_cnt)
print "Total Link : %d " %(link_cnt)
