import pickle
import math

"""
    Whole date/time range for the 2nd dataset: [1-DIC-2011 -> 28-APR-2012]: 150 days (3600h and 100h missing)
    
    How many ?:
    ===========
    Mondays : 21
    Tuesdays: 21
    Wednesdays: 21
    Thursdays: 22
    Fridays: 22
    Saturdays: 22
    Sundays: 21


"""

data = pickle.load(open( "tmp/save.p", "rb" ) )


f_out = open("tmp/data_commuting_analysis2.tsv", "w")
f_out.write("hour\tlen_lunes\ttot_lunes\tlen_martes\ttot_martes\tlen_miercoles\ttot_miercoles\tlen_jueves\ttot_jueves\tlen_viernes\ttot_viernes\tlen_sabado\ttot_sabado\tlen_domingo\ttot_domingo\n")
for hour in range(0,24):
    f_out.write("%s\t" % hour);
    for week_day in range(0,7):
        final_scape = ""
        if week_day != 6:
            final_scape = "\t"

        distances = data[week_day][hour]["length"]
        distances = [i for i in distances if i != 0]
        mean_distances = sum(distances)/len(distances)

        amounts = data[week_day][hour]["acum"]
        distances = [i for i in amounts if i != 0]
        mean_amounts = sum(amounts)/len(amounts)
        
#        # deviation
#        n = len(distances)
#        acum = 0
#        i = 0
#        while i < n:
#             acum += math.pow(distances[i] - mean_distances,2)
#             i += 1
#        deviation = math.sqrt(acum * (1/(n-1)))
        
        f_out.write("%s%s%s%s" % (mean_distances,"\t",mean_amounts,final_scape))
    f_out.write("\n")
f_out.close()