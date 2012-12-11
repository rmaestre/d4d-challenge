import pickle
import math 

data = pickle.load(open( "data_analysis.pkl", "rb" ) )

f_out = open("/tmp/data_commuting_analysis.tsv", "w")
f_out.write("hour\tlunes\tmartes\tmiercoles\tjueves\tviernes\tsabado\tdomingo\n")
for hour in range(1,22):
    f_out.write("%s\t" % hour);
    for week in range(0,7):
        scape = ""
        if week != 6:
            scape = "\t"
        mean = sum(data[week][hour])/(len(data[week][hour])-2)
        n = len(data[week][hour])-2
        acum = 0
        i = 0
        print(data[week][hour])
        while i < n:
             acum += math.pow(data[week][hour][i] - mean,2)
             print(n," ->",data[week][hour][i])
             i += 1
        deviation = math.sqrt(acum * (1/(n-1)))
        f_out.write("%s%s" % (mean,scape))
    f_out.write("\n")
f_out.close()