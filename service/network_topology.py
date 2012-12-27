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

# Data structure to save data per weeks
data = {}
for w in range(0,7):
    # 0=Monday, ..., 7=Sunday
    data[w] = {}
    for h in range(0,24):
        data[w][h] = {}
        data[w][h]["degree"] = []
        data[w][h]["acum"] = []

init_day = datetime(year, month, day, 0, 0, 0)
day_cont = 0
while day_cont < 7*5:
    print(init_day)
    # Perform analysis for each day
    for h in range(1,24):
        start = datetime(init_day.year, init_day.month, init_day.day, h, 0, 0)
        end =   datetime(init_day.year, init_day.month, init_day.day, h, 59, 59)
        # Debug info
        print(start," <<>>",end)  
        # Create user traces              
        users = {}
        for trace in __get_collection(config).find({'date': {'$gte': start, '$lt':end}}).sort('date',-1):
            if trace["userid"] not in users:
                users[trace["userid"]] = []
            if trace["antennaid"] != -1:
                users[trace["userid"]].append(trace["antennaid"])
        # Calculate antenna stancy
        for user in users:
            n = len(users[user])
            data[init_day.weekday()][h]["degree"].append(users[user])
    day_cont += 1
    init_day += timedelta(days=1)

pickle.dump( data, open( "/tmp/networks.p", "wb" ) )
