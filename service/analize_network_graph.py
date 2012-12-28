import math
import shapefile
from pymongo import *
from datetime import datetime, timedelta
import sys
import pickle
import itertools
import networkx as nx

graph = pickle.load(open("/tmp/networks.p", "rb"))

print(graph)