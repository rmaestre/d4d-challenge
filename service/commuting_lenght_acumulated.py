import math
import shapefile
from pymongo import *
from datetime import datetime, timedelta
import sys
import pickle

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
day = 2
month = 1
year = 2012

f_out = open("/tmp/output-%s_%s_%s.tsv" % (day, month, year), "w")
# Statistic vars
ci_values = []
print("%s\t%s\t%s" % ("hour", "lenght", "total"))
f_out.write("%s\t%s\t%s\n" % ("hour", "lenght", "total"))


# Data structure to save data per weeks
data = {}
for w in range(0,7):
    # 0=Monday, ..., 7=Sunday
    data[w] = {}
    for h in range(0,23):
        data[w][h] = []

init_day = datetime(year, month, day, 0, 0, 0)
day_cont = 0
while day_cont < 7*16:
    print(init_day)
    # Perform analysis for each day
    for h in range(1,23):
        start = datetime(init_day.year, init_day.month, init_day.day, h, 0, 0)
        end =   datetime(init_day.year, init_day.month, init_day.day, h+1, 59, 59)
        # Debug info
        print(start," <<>>",end)  
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
            data[init_day.weekday()][h].append(current_ci)
        except:
            print("%s\t%s\t%s" % (h, 0, 0))   
            data[init_day.weekday()][h].append(0)
        print(data)
    day_cont += 1
    init_day += timedelta(days=1)
    pickle.dump( data, open( "/tmp/save%s.p" % day_cont, "wb" ) )

pickle.dump( data, open( "/tmp/save.p", "wb" ) )

