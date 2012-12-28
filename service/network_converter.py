import math
import shapefile
from pymongo import *
from datetime import datetime, timedelta
import sys
import pickle
import itertools
import networkx as nx

graph = pickle.load(open("/tmp/networks.p", "rb"))

# Print header info
print("hour\tid_antenna_from\tid_antenna_to\tweight")
for hour in range(0,24):
    for edge in graph[hour].edges():
        print("%s\t%s\t%s\t%s" % (hour, edge[0], edge[1], graph[hour][edge[0]][edge[1]]["weight"]))