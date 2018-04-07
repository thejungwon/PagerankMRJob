with open('wikipedia-av-html.tar') as f:
    lines = f.readlines()
    cnt = 0
    for line in lines:

        if "av/articles/" in line:
            cnt+=1
            print(line)

        if cnt == 3:
            break
