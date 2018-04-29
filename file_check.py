unique_links = set()
with open("result.txt") as f:
    lines= f.readlines()
    for line in lines :
        # print(line)
        line=line.replace('"','')
        line=line.split("\t")

        unique_links.add(line[0].split("/")[0])
        for li in line[1].split():

            unique_links.add(li)



    print(len(set(unique_links)))

