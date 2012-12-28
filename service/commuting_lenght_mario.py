import math
import shapefile
from pymongo import *
from datetime import datetime

"""
Somos más exigentes en cuanto a lo que consideramos movimiento de commuters.
Ubicamos todas las antenas de una misma subprefectura en su centroide, ergo transiciones entre antenas de una misma subpref. de distancia nula


"""

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
        result = math.sqrt(math.pow((x[0]-y[0]),2) + math.pow((x[1]-y[1]),2))
        return result

# Load antenna-subprefecture correspondence
antenna_subpref = {}
with open("../geo_layers/antenna_subprefecture.txt", 'r') as f:
    for line in f:
        data = line.split()
        antenna_subpref[data[0]] = data[1]

# Load subprefecture centroids
centroids = {}
with open("../geo_layers/centroids.csv", 'r') as f:
    for line in f:
        data = line.split(',')
        centroids[data[2].replace('\n', '')] = (data[0], data[1])

# Load atenna positions
antennas = {}
for line in open("../rawdata/ANT_POS.TSV" , 'r'):
    line = line.replace("\n","")
    chunks = line.split("\t")
    if len(chunks) == 3:
        antenna_id = antenna_subpref.get(chunks[0], '-1')
        if antenna_id != '-1':
            antennas[antenna_id] = [float(centroids[antenna_id][0]), float(centroids[antenna_id][1])]
antennas['-1'] = (-1,-1)


# Specific month and day to start the search
day = 23
month = 2

print("%s\t%s\t%s" % ("hour", "lenght", "total"))
# Perform analysis for each day
for h in range(1,23):
    start = datetime(2012, month, day, h, 0, 0)
    end =   datetime(2012, month, day, h+1, 59, 59)
    # Debug info
    #print(start," ",end)  
    # Create user traces              
    users = {}
    for trace in __get_collection(config).find({'date': {'$gte': start, '$lt':end}}):
        if trace["userid"] not in users:
            users[trace["userid"]] = []
        users[trace["userid"]].append(antenna_subpref.get(str(trace["antennaid"]), '-1'))
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
            if antennas[users[user][i]][0] != '-1' and antennas[users[user][i+1]][0] != '-1':
                users_final[user]["trace_points"].append(antennas[users[user][i]])
                acum += get_distance(antennas[users[user][i]], antennas[users[user][i+1]])
                antenna_cont += 1
            i += 1
        users_final[user]["trace_lenght"] = acum
    # Return the mean
    for user in users_final:
        acum += users_final[user]["trace_lenght"]
    try:
        print("%s\t%s\t%s" % (h, acum/antenna_cont, len(users_final)))
    except:
        print("%s\t%s\t%s" % (h, 0, 0))
    
    
    
"""
# Save user traces into a SHP file
w_traces_long = shapefile.Writer(shapefile.POLYLINE)
w_traces_long.field('FIRST_FLD','C','40')

w_traces_short = shapefile.Writer(shapefile.POLYLINE)
w_traces_short.field('FIRST_FLD','C','40')

for user in users_final:
    l_points = []
    for point in users_final[user]["trace_points"]:
        l_points.append([point[0],point[1]])
        
    if len(l_points) > 2:
        #print(users_final[user]["trace_lenght"])
        #print(users_final[user]["trace_points"])
        if users_final[user]["trace_lenght"] > 10:
            w_traces_long.line(parts = [l_points])
            w_traces_long.record(FIRST_FLD = users_final[user]["trace_lenght"])
        else:
            w_traces_short.line(parts = [l_points])
            w_traces_short.record(FIRST_FLD = users_final[user]["trace_lenght"])
    else:
        print(l_points)
            
# Save shapefile
w_traces_long.save('/tmp/cl_long.shp')
w_traces_short.save('/tmp/cl_short.shp')
"""







  