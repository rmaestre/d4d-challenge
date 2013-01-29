# -*- coding: utf-8 -*-
"""
    This class calculates a 24h graph (pickle file) for a specific week-day, basing on traces from 2nd Dataset. 
    '-1' antennas and 'stays' have been ignored, so that only 'transitions' between different located antennas are considered.
    Moreover, traces are gathered in chronological order and (a,b) & (b,a) edges are differently considered
    
    --> By changing the variable WEEK_DAY (0=Monday, ...., 6=Sunday), the calculations will be done 

    http://networkx.lanl.gov/reference/classes.digraph.html
    
    Created 24/01/2013
    
    @author: Paradigma Labs
"""
from datetime import datetime, timedelta
from pymongo import *
import itertools
import math
import networkx as nx
import pickle
import shapefile
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
    
    
# Load antennas positions
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
day = 1
month = 12
year = 2011

init_day = datetime(year, month, day, 0, 0, 0)
cont_day = 0

# Iterate over the whole data sets (7*16)
while cont_day < 7*16:
    print(init_day)
    
    # 0=Monday, ...., 6=Sunday
    WEEK_DAY = 6
    if init_day.weekday() != WEEK_DAY:
        print("Jumping day!")
    else:
        # Perform analysis for each day
        for h in range(1,24):
            print(h)
            start = datetime(init_day.year, init_day.month, init_day.day, h, 0, 0)
            end =   datetime(init_day.year, init_day.month, init_day.day, h, 59, 59)
            
            users = {}
            # Iterate over the mongoDB with a specific HOUR range saving user traces
            for trace in __get_collection(config).find( {'date': {'$gte': start, '$lt':end}}).sort('date', 1):
                if trace["userid"] not in users:
                    users[trace["userid"]] = []
                if trace["antennaid"] != -1: # removing -1 antennas
                    users[trace["userid"]].append(trace["antennaid"])
            
            # Flat user traces (replace list of antennas repeated)
            for user in users:
                # Check if there is any antenna value
                if len(users[user]) > 1:
                
                    # Add first antenna position
                    flatten_trace = [users[user][0]]
                
                    # Flat list
                    list_lenght = len(users[user])
                    index = 0
                    while index < list_lenght - 1:
                        if users[user][index] != users[user][index + 1]: # ignoring repeated antennas ('stays'), only 'transitions' are taken into account
                            flatten_trace.append(users[user][index + 1]) 
                        index += 1
                
                    # If we have one edge at least ( -> 2 nodes), is a minimum and valid subgraph
                    # and it will processed into a hash-graph structure
                    if len(flatten_trace) > 1:
                        #print("User: %s\tTrace: %s" % (user,flatten_trace))
                        list_lenght = len(flatten_trace)
                        index = 0
                        while index < list_lenght - 1:
                            # Added new node_from If previously node_from does not exists
                            if flatten_trace[index] not in graph[h].nodes():
                                graph[h].add_node(flatten_trace[index])
                            # Added new node_to If previously node_to does not exists
                            if flatten_trace[index + 1] not in graph[h].nodes():
                                graph[h].add_node(flatten_trace[index + 1])
                            # Added new edge between nodes If previously edge does not exists
                            # with weight = 1
                            if (flatten_trace[index], flatten_trace[index + 1]) not in graph[h].edges():
                                graph[h].add_edge(flatten_trace[index], flatten_trace[index + 1], weight = 1)
                            else:
                                # Get existent edge and increment one value its current weight
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
    
pickle.dump(graph, open("/tmp/networks%s.p" % WEEK_DAY, "wb"))
    
    
    
