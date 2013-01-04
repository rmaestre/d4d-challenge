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
    
    
def distance_on_unit_sphere(x,y):
    """ http://www.johndcook.com/python_longitude_latitude.html """
    
    if x[0] == -1 or y[0] ==-1:
        return 0.0

    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0
        
    # phi = 90 - latitude
    phi1 = (90.0 - x[1])*degrees_to_radians
    phi2 = (90.0 - y[1])*degrees_to_radians
        
    # theta = longitude
    theta1 = x[0]*degrees_to_radians
    theta2 = y[0]*degrees_to_radians
        
    # Compute spherical distance from spherical coordinates.
        
    # For two locations in spherical coordinates 
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) = 
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length
    
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + 
           math.cos(phi1)*math.cos(phi2))
    if cos > 1: # sometimes, numerical operations return a cos  1.00000002 > 1 math domain error !!
        cos = 1
    arc = math.acos( cos )

    # Remember to multiply arc by the radius of the earth 
    # in your favorite set of units to get length.
    return arc * 6373 # km


    
def get_distance(x,y):
    """
    Return euclidean distance for UTM coordinates
    http://www.elgps.com/mensajes/distanciautm.html
    """
    if x[0] == -1 or y[0] ==-1:
        return 0.0
    else:
        return math.sqrt(math.pow((x[0]-y[0]),2) + math.pow((x[1]-y[1]),2))
        
# Load antenna positions
antennas = {}
for line in open("ANT_POS.TSV" , 'r'):
    line = line.replace("\n","")
    chunks = line.split("\t")
    if len(chunks) == 3:
        antenna_id = int(chunks[0])
        antennas[antenna_id] = [float(chunks[1]),float(chunks[2])]
antennas[-1] = (-1,-1)

# Data structure to save data per week-day
data = {}
for w in range(0,7):
    # 0=Monday, ..., 6=Sunday
    data[w] = {}
    for h in range(0,24):
        data[w][h] = {}
        data[w][h]["length"] = []
        data[w][h]["acum"] = []

# [1-DIC-2011 -> 28-APR-2012]: 150 days (3600h and 100h missing)
init_day = datetime(2011, 12, 1, 0, 0, 0)
day_cont = 0
while day_cont < 150:
    # Perform analysis for each day
    for h in range(0,24):
        start = datetime(init_day.year, init_day.month, init_day.day, h, 0, 0)
        end =   datetime(init_day.year, init_day.month, init_day.day, h, 59, 59)
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
        dynamic_users_cont = 0 # only those ones who really change their location during this hour
        users_final = {}
        for user in users:
            users_final[user] = {}
            users_final[user]["trace_points"] = []
            n = len(users[user]) # amount of calls <-> antennas for this customer and this hour
            i = 0
            acum = 0
            while i < n - 1: # since each iteration is for i and i+1
                if antennas[users[user][i]][0] != -1 and antennas[users[user][i+1]][0] != -1:
                    users_final[user]["trace_points"].append(antennas[users[user][i]])
                    acum += distance_on_unit_sphere(antennas[users[user][i]], antennas[users[user][i+1]])
                    antenna_cont += 1
                i += 1
            users_final[user]["trace_lenght"] = acum
            if acum > 0:
                dynamic_users_cont += 1
        acum = 0
        for user in users_final:
            acum += users_final[user]["trace_lenght"]
        try:
            # Save max and min commuting index
            current_ci = acum/dynamic_users_cont
            # Dump information
#            print("%s\t%s\t%s" % (h, current_ci, dynamic_users_cont))
            data[init_day.weekday()][h]["length"].append(current_ci)
            data[init_day.weekday()][h]["acum"].append(dynamic_users_cont)
        except:
#            print("%s\t%s\t%s" % (h, 0, 0))   
            data[init_day.weekday()][h]["length"].append(0)
            data[init_day.weekday()][h]["acum"].append(0)
#        print(data)
    day_cont += 1
    init_day += timedelta(days=1)
#    pickle.dump( data, open( "tmp/save%s.p" % day_cont, "wb" ) ) # partial backup

pickle.dump( data, open( "tmp/save.p", "wb" ) )

