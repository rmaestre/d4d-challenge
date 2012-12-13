import math 

data = {}
line_count = 0
for line in open("/tmp/data_commuting_analysis.tsv", "r"):
    if line_count not in data:
        data[line_count] = {}
        for i in range(0,14):
            data[line_count][i] = 0
    line = line.replace("\n","")
    chunks = line.split("\t")
    if len(chunks) == 15:
        column_count = 0
        for chunk in chunks:
            data[line_count][column_count] = chunk
            column_count += 1
    line_count += 1
    

week = 5
i = 2
while i < 23:
    print("%d\t%.5f\t%.5f" % (i, float(data[i][week]), float(data[i][week])-float(data[i-1][week])))
    i += 1