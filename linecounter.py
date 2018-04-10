from sys import argv
with open(argv[1]) as f:
    lines = f.readlines()
    print(len(lines))
