import math
import shapefile
from pymongo import *
from datetime import datetime
import sys

config = {}
config['db'] = {}
config['db']['host'] = "web40"
config['db']['port'] = 27017
config['db']['db'] = "d4dchallenge"
config['db']['collection'] = "traces"


def __get_collection(config):
    """
    Return collection from MongoDB with a specific configuration
    """
    connection = Connection(config['db']['host'], config['db']['port'])
    db = connection[config['db']['db']]
    collection = db[config['db']['collection']]
    return collection
    
    
def get_distance(x,y):
    """
    Return euclidean distance for UTM coordinates
    http://www.elgps.com/mensajes/distanciautm.html
    """
    if x[0] == -1 or y[0] ==-1:
        return 0.0
    else:
        return math.sqrt(math.pow((x[0]-y[0]),2) + math.pow((x[1]-y[1]),2))
        
# Load atenna positions
antennas = {}
for line in open("../rawdata/ANT_POS.TSV" , 'r'):
    line = line.replace("\n","")
    chunks = line.split("\t")
    if len(chunks) == 3:
        antenna_id = int(chunks[0])
        antennas[antenna_id] = [float(chunks[1]),float(chunks[2])]
antennas[-1] = (-1,-1)

# Specific month and day to start the search
day = 27
month = 3
year = 2012

f_out = open("/tmp/output-%s_%s_%s.tsv" % (day, month, year), "w")
# Statistic vars
ci_values = []
print("%s\t%s\t%s" % ("hour", "lenght", "total"))
f_out.write("%s\t%s\t%s\n" % ("hour", "lenght", "total"))

# Perform analysis for each day
for h in range(1,23):
    start = datetime(year, month, day, h, 0, 0)
    end =   datetime(year, month, day, h+1, 59, 59)
    # Debug info
    #print(start," ",end)  
    # Create user traces              
    users = {}
    for trace in __get_collection(config).find({'date': {'$gte': start, '$lt':end}}):
        if trace["userid"] not in users:
            users[trace["userid"]] = []
        users[trace["userid"]].append(trace["antennaid"])
    # Calculate max length and user distance
    antenna_cont = 0
    users_final = {}
    for user in users:
        users_final[user] = {}
        users_final[user]["trace_points"] = []
        n = len(users[user])
        i = 0
        acum = 0
        points = []
        while i < n - 1:
            if antennas[users[user][i]][0] != -1 and antennas[users[user][i+1]][0] != -1:
                users_final[user]["trace_points"].append(antennas[users[user][i]])
                acum += get_distance(antennas[users[user][i]], antennas[users[user][i+1]])
                antenna_cont += 1
            i += 1
        users_final[user]["trace_lenght"] = acum
    # Return the mean
    for user in users_final:
        acum += users_final[user]["trace_lenght"]
    try:
        # Save max and min commuting index
        current_ci = acum/antenna_cont
        ci_values.append(current_ci)
        # Dump information
        print("%s\t%s\t%s" % (h, current_ci, antenna_cont))
        f_out.write("%s\t%s\t%s\n" % (h, current_ci, antenna_cont))
    except:
        print("%s\t%s\t%s" % (h, 0, 0))
        f_out.write("%s\t%s\t%s\n" % (h, 0, 0))
f_out.close()

# Calulate commuting index avg
commuting_index_avg = (max(ci_values) - min(ci_values))/2    

# Save user traces into a SHP file
# long one
w_traces_long = shapefile.Writer(shapefile.POLYLINE)
w_traces_long.field('WEIGHT','N','40')
# short one
w_traces_short = shapefile.Writer(shapefile.POINT)
w_traces_short.field('WEIGHT','N','40')

antenna_weight = {}
print("Max:%s min:%s avg:%s" % (max(ci_values), min(ci_values), commuting_index_avg))
# Iterate over the users traces
longs = 0
shorts = 0
for user in users_final:
    # Create a list of points
    l_points = []
    for point in users_final[user]["trace_points"]:
        l_points.append([point[0],point[1]])
    # If we have enougth information
    if len(l_points) > 1:
        if users_final[user]["trace_lenght"] > commuting_index_avg:
            longs += 1
            w_traces_long.line(parts = [l_points])
            w_traces_long.record(WEIGHT = users_final[user]["trace_lenght"])
        else:
            shorts += 1
            for point in l_points:
                point = (point[0],point[1])
                if point not in antenna_weight:
                    antenna_weight[point] = 1
                else:
                    antenna_weight[point] += 1
# Print stats                
print("Moving:%.2s  Static:%.2s" % ((longs*100)/(longs+shorts), (shorts*100)/(longs+shorts)))

# Save points
for antenna in antenna_weight:
    w_traces_short.point(antenna[0],antenna[1])
    w_traces_short.record(WEIGHT = math.log(antenna_weight[antenna]))
    
# Save shapefiles
w_traces_long.save('/tmp/cl_long.shp')
w_traces_short.save('/tmp/cl_short.shp')


