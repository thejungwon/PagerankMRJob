from pyhive import hive
cursor = hive.connect('localhost').cursor()
cursor.execute("SELECT * FROM `wiki` WHERE `id` LIKE '%.htmm%' OR `id` LIKE '%.htmk%'  ORDER BY `id` ASC")
strs = cursor.fetchall()

for i in range(0,len(strs),2):
    pair_found = False
    newname=""
    newlinks=""
    if '.htmm' in strs[i][0]:
        if '.htmk' in strs[i+1][0]:
            newname = newrow[0].replace("htmm","html").encode('utf-8')
            originCnt = int(newname.split("/")[-1].replace('"',''))
            newCnt = originCnt + int(strs[i+1][0].split("/")[-1].replace('"',''))
            newname = newname.replace("/" + str(originCnt), "/" + str(newCnt))
            newlinks = '"'+str(strs[i][1].encode('utf-8').replace('"','')+" "+strs[i+1][1].encode('utf-8').replace('"','')).strip()+'"'
    elif '.htmk' in strs[i][0]:
        if '.htmm' in strs[i+1][0]:
            newname = strs[i+1][0].replace("htmm", "html").encode('utf-8')
            originCnt=int(newname.split("/")[-1].replace('"',''))
            newCnt=originCnt+int(strs[i][0].split("/")[-1].replace('"',''))
            newname=newname.replace("/"+str(originCnt),"/"+str(newCnt))
            newlinks= '"'+str(strs[i+1][1].encode('utf-8').replace('"','')+" "+strs[i][1].encode('utf-8').replace('"','')).strip()+'"'
    newrow = newname+"\t"+newlinks
    print(newrow)
