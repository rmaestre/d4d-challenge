# -*- coding: utf-8 -*-
"""
    This class processes the output file (pickle) returned by network_creation.py, adding lon/lat coordinates for each antenna.
    Only 'dest_antennas' are considered for weight, that is, only 'inlinks', and the results are being expressed in logarithmic scale (2 basis).
    
    
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


# Load antennas positions
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
        
        
graph = pickle.load(open("/tmp/networks6.p", "rb"))

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
        if edge[1] not in weights: # only 'node_to' taken into account
            weights[edge[1]] = 0
        weights[edge[1]] += graph[hour][edge[0]][edge[1]]["weight"]
        
    fd_out = open("/tmp/D_hour_%s.tsv" % hour, "w")
    fd_out.write("antenna_lon\tantenna_lat\tweight\n")
    for antenna in weights:
        if weights[antenna] > 0:
            fd_out.write("%s\t%s\t%s\n" % (antennas[antenna][0], antennas[antenna][1], math.log(weights[antenna], 2)))
    fd_out.close()

    
    
    
    
    
    


