import math
import shapefile
from pymongo import *
from datetime import datetime, timedelta
import sys
import pickle
import itertools
import networkx as nx

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
    
    
# Load atennas positions
antennas = {}

# Load hte default antenna
antennas[-1] = (-1, -1)

# Iterate over the TSV file
for line in open("../rawdata/ANT_POS.TSV" , 'r'):
    # Remove special characters
    line = line.replace("\n","")
    chunks = line.split("\t")
    
    # If line is valid
    if len(chunks) == 3:
        antenna_id = int(chunks[0])
        antennas[antenna_id] = [float(chunks[1]), float(chunks[2])]

# Data structure to save data per month of day
graph = {}
for h in range(0,24):
    graph[h] = nx.DiGraph()
    
# Specific month and day to start the search
day = 2
month = 1
year = 2012

init_day = datetime(year, month, day, 0, 0, 0)
cont_day = 0

# Iterate over the whole data sets (7*16)
while cont_day < 7*16:
    print(init_day)
    
    if init_day.weekday() != 0:
        print("Jumping day")
    else:
        # Perform analysis for each day
        for h in range(1,24):
            print(h)
            start = datetime(init_day.year, init_day.month, init_day.day, h, 0, 0)
            end =   datetime(init_day.year, init_day.month, init_day.day, h, 59, 59)
            
            users = {}
            # Iterate over the mongoDB with a specific HOUR range saving user traces
            for trace in __get_collection(config).find( {'date': {'$gte': start, '$lt':end}}).sort('date', -1):
                if trace["userid"] not in users:
                    users[trace["userid"]] = []
                if trace["antennaid"] != -1:
                    users[trace["userid"]].append(trace["antennaid"])
            
            # Flat user traces (removing -1 antennas id, and replace list of antennas repetead)
            for user in users:
                # Check if there is any antenna valus
                if len(users[user]) > 1:
                
                    # Add first antenna position
                    flatten_trace = [users[user][0]]
                
                    # Flat list of repetead values
                    list_lenght = len(users[user])
                    index = 0
                    while index < list_lenght - 1:
                        if users[user][index] != users[user][index + 1]:
                            flatten_trace.append(users[user][index + 1])
                        index += 1
                
                    # If we have one edge at least, is a minimun and valid subgraph
                    # and it will processed into a hash-graph structure
                    if len(flatten_trace) > 1:
                        #print("User: %s\tTrace: %s" % (user,flatten_trace))
                        list_lenght = len(flatten_trace)
                        index = 0
                        while index < list_lenght - 1:
                            if flatten_trace[index] not in graph[h].nodes():
                                graph[h].add_node(flatten_trace[index])
                            if flatten_trace[index + 1] not in graph[h].nodes():
                                graph[h].add_node(flatten_trace[index + 1])
                            if (flatten_trace[index], flatten_trace[index + 1]) not in graph[h].edges():
                                graph[h].add_edge(flatten_trace[index], flatten_trace[index + 1], weight = 1)
                            else:
                                edge_weight = graph[h][flatten_trace[index]][flatten_trace[index + 1]]["weight"]
                                graph[h].add_edge(flatten_trace[index], 
                                                    flatten_trace[index + 1], 
                                                    weight = edge_weight + 1)
                            index += 1
                        
        # Print debug info
        print("Total nodes: %s" % len(graph[h].nodes()))
        print("Total edges: %s" % len(graph[h].edges()))
    
    # Next loop variables
    cont_day+=1
    init_day += timedelta(days=1)
    
pickle.dump(graph, open("/tmp/networks.p", "wb"))
graph = pickle.load(open("/tmp/networks.p", "rb"))
    
    
    
