import math
import shapefile
from pymongo import *
from datetime import datetime, timedelta
import sys
import pickle
from test.datetimetester import DAY

"""
    Studying the whole 2nd dataset, we are trying to get as many values related to the commuting as possible
    
    Assumptions:
    a) -1 antennas are not taken into account for the distance calculation, buy they're for calls
    b) No problem for appending each 50.000 users group every 15 days, since the it's very unlikely to have the same ID for 2 customers,
    when the 15-days period (TSV) change and at the same hour --> check max_distance_for_a_single_transition
    c) 
    
    
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
    
    
def distance_on_unit_sphere(x,y):
    """ http://www.johndcook.com/python_longitude_latitude.html """

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


    
#def get_distance(x,y):
#    """
#    Return euclidean distance for UTM coordinates
#    http://www.elgps.com/mensajes/distanciautm.html
#    """
#    return math.sqrt(math.pow((x[0]-y[0]),2) + math.pow((x[1]-y[1]),2))
        
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
        # each item is a list containing the corresponding value for all customers in that hour
        # list size is the amount of Mondays, Tuesdays... contained in the 2nd dataset period 
#        data[w][h]["length"] = []
#        data[w][h]["acum"] = []
        data[w][h]["distances"] = []
        data[w][h]["transitions"] = []
        data[w][h]["calls"] = []
        data[w][h]["users"] = []
        data[w][h]["dynamic_users"] = []
        data[w][h]["static_users"] = []


# [1-DIC-2011 -> 28-APR-2012]: 150 days (3600h and 100h missing)
init_day = datetime(2011, 12, 1, 0, 0, 0)
day_cont = 0
max_distance_for_a_single_transition = 0
while day_cont < 150: # Perform analysis for each day
    if init_day.weekday() == 0:
        print ("Processing day %d of a total of 150 days" % day_cont)
        for h in range(0,24):
            start = datetime(init_day.year, init_day.month, init_day.day, h, 0, 0)
            end =   datetime(init_day.year, init_day.month, init_day.day, h, 59, 59)
            # Create user traces              
            users = {}
            for trace in __get_collection(config).find({'date': {'$gte': start, '$lt':end}}): #.sort([('date', 1)])
                if trace["userid"] not in users:
                    users[trace["userid"]] = []
    #            if trace["antennaid"] != -1: # -1 traces ignored
    #                users[trace["userid"]].append(trace["antennaid"])
                users[trace["userid"]].append(trace["antennaid"])
                    
            # Calculate max length and user distance
            located_antennas_amount = 0
            users_amount = len(users)
            dynamic_users_cont = 0 # only those ones who really change their location during this hour
            static_users_cont = 0
            users_final = {}
            
            for user in users:
                users_final[user] = {}
    #            users_final[user]["trace_points"] = []
                calls = len(users[user]) # amount of calls for this customer and this hour
                transitions = 0
                i = 0
                dist = 0
                while i < calls - 1: # since each iteration is for i and i+1
                    if antennas[users[user][i]][0] != -1 and antennas[users[user][i+1]][0] != -1:
    #                    users_final[user]["trace_points"].append(antennas[users[user][i]])
                        dist_i = distance_on_unit_sphere(antennas[users[user][i]], antennas[users[user][i+1]]) 
                        dist += dist_i
                        if dist_i > 0:
                            transitions += 1 # minimum level for customers with -1 antennas
                        if dist_i > max_distance_for_a_single_transition:
                            print(antennas[users[user][i]], antennas[users[user][i+1]])
                            max_distance_for_a_single_transition = dist_i
                        located_antennas_amount += 1
                    i += 1
                users_final[user]["distance"] = dist
                users_final[user]["transitions"] = transitions
                users_final[user]["calls"] = calls
                if dist > 0:
                    dynamic_users_cont += 1
                else:
                    static_users_cont += 1
            # 'total_xxx' for all customers in this hour
            total_dist = 0 
            total_transitions = 0
            total_calls = 0
            for user in users_final:
                total_dist += users_final[user]["distance"]
                total_transitions += users_final[user]["transitions"]
                total_calls += users_final[user]["calls"]
            try:
    #            print("%s\t%s\t%s" % (h, current_ci, dynamic_users_cont))
                data[init_day.weekday()][h]["distances"].append(total_dist)
                data[init_day.weekday()][h]["transitions"].append(total_transitions)
                data[init_day.weekday()][h]["calls"].append(total_calls)
                data[init_day.weekday()][h]["users"].append(users_amount)
                data[init_day.weekday()][h]["dynamic_users"].append(dynamic_users_cont)
                data[init_day.weekday()][h]["static_users"].append(static_users_cont)
            except Exception as e:
                print(">>>>>>>>>>>>>>>>>> ERROR%s\t%s\t%s" % (h, 0, 0))
                print(e)   
    #            data[init_day.weekday()][h]["length"].append(0)
    #            data[init_day.weekday()][h]["acum"].append(0)
    #        print(data)
    day_cont += 1
    init_day += timedelta(days=1)
#    pickle.dump( data, open( "tmp/save%s.p" % day_cont, "wb" ) ) # partial backup

pickle.dump( data, open( "tmp/dataset2_matrix.pyc", "wb" ) )
print ("max_distance_for_a_single_transition: %s" % max_distance_for_a_single_transition)
