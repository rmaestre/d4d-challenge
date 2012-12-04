import math
import shapefile
from pymongo import *
from datetime import datetime


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
day = 6
month = 3

print("%s\t%s" % ("hour", "lenght"))
# Perform analysis for each day
for h in range(1,23):
    start = datetime(2012, month, day, h, 0, 0)
    end =   datetime(2012, month, day, h+1, 0, 0)
    # Debug info
    #print(start," ",end)  
    # Create user traces              
    users = {}
    for trace in __get_collection(config).find({'date': {'$gte': start, '$lt':end}}):
        if trace["userid"] not in users:
            users[trace["userid"]] = []
        users[trace["userid"]].append(trace["antennaid"])
    # Calculate max length and user distance
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
            i += 1
        users_final[user]["trace_lenght"] = acum
    # Return the mean
    for user in users_final:
        acum += users_final[user]["trace_lenght"]
    print("%s\t%s" % (h, (acum/len(users_final))))
    
    
    
"""
# Save user traces into a SHP file
w_traces = shapefile.Writer(shapefile.POLYLINE)
w_traces.field('FIRST_FLD','C','40')
for user in users_final:
    l_points = []
    for point in users_final[user]["trace_points"]:
        l_points.append([point[0],point[1]])
    if len(l_points) > 0 and users_final[user]["trace_lenght"] > 4:
        print(users_final[user]["trace_lenght"])
        w_traces.line(parts = [l_points])
        w_traces.record(FIRST_FLD = users_final[user]["trace_lenght"])
# Save shapefile
w_traces.save('/tmp/commuting_polylines.shp')
"""






  