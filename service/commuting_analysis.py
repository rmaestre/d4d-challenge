import pickle
import math 

data = pickle.load(open( "data_analysis_complete.pkl", "rb" ) )


f_out = open("/tmp/data_commuting_analysis.tsv", "w")
f_out.write("hour\tlen_lunes\ttot_lunes\tlen_martes\ttot_martes\tlen_miercoles\ttot_miercoles\tlen_jueves\ttot_jueves\tlen_viernes\ttot_viernes\tlen_sabado\ttot_sabado\tlen_domingo\ttot_domingo\n")
for hour in range(1,23):
    f_out.write("%s\t" % hour);
    for week in range(0,7):
        final_scape = ""
        if week != 6:
            final_scape = "\t"
        mean = sum(data[week][hour]["length"])/(len(data[week][hour]["length"])-2)
        n = len(data[week][hour])-2
        acum = 0
        i = 0
        while i < n:
             acum += math.pow(data[week][hour]["length"][i] - mean,2)
             i += 1
        deviation = math.sqrt(acum * (1/(n-1)))
        f_out.write("%s%s%s%s" % (mean,"\t",sum(data[week][hour]["acum"]),final_scape))
    f_out.write("\n")
f_out.close()