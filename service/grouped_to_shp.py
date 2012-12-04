#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class read a pickle dict and create a SHP representing 
    user communitacions
    
    Created 03/12/2012
    
    @author: Paradigma Labs
"""
import shapefile
import tarfile
import pickle
import math

# Load antennas
antennas = {}
for line in open("../rawdata/ANT_POS.TSV" , 'r'):
    line = line.replace("\n","")
    chunks = line.split("\t")
    if len(chunks) == 3:
        antenna_id = int(chunks[0])
        antennas[antenna_id] = {}
        antennas[antenna_id]["lon"] = float(chunks[1])
        antennas[antenna_id]["lat"] = float(chunks[2])
## insert default antenna
antennas[-1] = {}
antennas[-1]["lon"] = -1
antennas[-1]["lat"] = -1

w_traces = shapefile.Writer(shapefile.POLYLINE)
w_traces.field('FIRST_FLD','C','40')

f = open('graph_grouped11-15.pkl', 'rb')
graph = pickle.load(f)
for node_from in graph:
    for node_to in graph[node_from]:
        if (antennas[node_from]["lat"] != -1 and antennas[node_from]["lon"] != -1
            and antennas[node_to]["lat"] != -1 and antennas[node_to]["lon"] != -1):
            if graph[node_from][node_to] > 20:
                w_traces.line(parts = [[[antennas[node_from]["lon"],antennas[node_from]["lat"]],[antennas[node_to]["lon"],antennas[node_to]["lat"]]]])
                w_traces.record(FIRST_FLD = graph[node_from][node_to])

w_traces.save('/tmp/commuting_grouped_polylines_11_15.shp')

w = shapefile.Writer(shapefile.POINT)
w.field('FIRST_FLD')
w.field('SECOND_FLD','C','40')
nodes_weight = {}
for node_from in graph:
    for node_to in graph[node_from]:
        if (antennas[node_from]["lat"] != -1 and antennas[node_from]["lon"] != -1
            and antennas[node_to]["lat"] != -1 and antennas[node_to]["lon"] != -1):
            if node_to not in nodes_weight:
                nodes_weight[node_to] = graph[node_from][node_to]
            else:
                nodes_weight[node_to] += graph[node_from][node_to]
                
w = shapefile.Writer(shapefile.POINT)
w.field('FIRST_FLD')
for node in nodes_weight:
    w.point(antennas[node]["lon"], antennas[node]["lat"])
    w.record(FIRST_FLD = math.log(nodes_weight[node]))
        
w.save('/tmp/commuting_grouped_nodes_11_15.shp')
        