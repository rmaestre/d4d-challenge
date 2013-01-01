import math
import shapefile
from pymongo import *
from datetime import datetime, timedelta
import sys
import pickle
import itertools
import networkx as nx


# Load atennas positions
antennas = {}
weights = {}

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
        weights[antenna_id] = 0
        antennas[antenna_id] = [float(chunks[1]), float(chunks[2])]
        
        
graph = pickle.load(open("/tmp/networks1.p", "rb"))

"""
# Print header info
print("hour\tid_antenna_from\tid_antenna_to\tweight")
for hour in range(0,24):
    for edge in graph[hour].edges():
        print("%s\t%s\t%s\t%s" % (hour, edge[0], edge[1], graph[hour][edge[0]][edge[1]]["weight"]))     
"""

for hour in range(0,24):
    
    weights = {}
    for edge in graph[hour].edges():
        if edge[1] not in weights:
            weights[edge[1]] = 0
        weights[edge[1]] += graph[hour][edge[0]][edge[1]]["weight"]
        
    fd_out = open("/tmp/hour_%s.tsv" % hour, "w")
    fd_out.write("antenna_lon\tantenna_lat\tweight\n")
    for antenna in weights:
        if weights[antenna] > 0:
            fd_out.write("%s\t%s\t%s\n" % (antennas[antenna][0], antennas[antenna][1], math.log(weights[antenna], 2)))
    fd_out.close()

    
    
    
    
    
    


