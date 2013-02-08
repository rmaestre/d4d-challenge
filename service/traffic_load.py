# -*- coding: utf-8 -*-
"""
    http://networkx.lanl.gov/reference/classes.digraph.html
    
    Created 24/01/2013
    
    @author: Paradigma Labs
"""
import math
import networkx as nx
import pickle

# Firstly, load antenna ids and lat/lon
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
        
        
hours_graph = pickle.load( open( "../results_kernel_density/Martes/networks1.p", "rb" ) )


for graph in hours_graph:
    f_out = open("../results_traficc_segments/Martes/traffic_intensity%s.tsv" % graph, "w")
    for edge in hours_graph[graph].edges():
        antenna_from = antennas[edge[0]]
        antenna_to = antennas[edge[1]]
        print(hours_graph[graph][edge[0]][edge[1]]['weight'])
        if hours_graph[graph][edge[0]][edge[1]]['weight'] != 0:
            f_out.write("%s\t%s\t%s\t%s\t%s\n" % (antenna_from[0],  antenna_from[1],
                                    antenna_to[0], antenna_to[1], 
                                    hours_graph[graph][edge[0]][edge[1]]['weight']))
        else:
            print(hours_graph[graph][edge[0]][edge[1]]['weight'])
f_out.close()