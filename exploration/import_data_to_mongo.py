from pymongo import Connection
from datetime import datetime

config = {}
config['db'] = {}
config['db']['host'] = "web40"
config['db']['port'] = 27017
config['db']['db'] = "d4dchallenge"

def __get_collection(config, collection_name):
    connection = Connection(config['db']['host'], config['db']['port'])
    db = connection[config['db']['db']]
    collection = db[collection_name]
    return collection

## DATASET 2 - ANTENNAS
#
## Get collection
#traces = __get_collection(config, "traces")
#
##  Insert antennas id into a hash to get lon/lat
#antennas = {}
#for line in open("../rawdata/ANT_POS.TSV" , 'r'):
#    line = line.replace("\n","")
#    chunks = line.split("\t")
#    if len(chunks) == 3:
#        antenna_id = int(chunks[0])
#        antennas[antenna_id] = {}
#        antennas[antenna_id]["lon"] = float(chunks[1])
#        antennas[antenna_id]["lat"] = float(chunks[2])
## insert default antenna
#antennas[-1] = {}
#antennas[-1]["lon"] = -1
#antennas[-1]["lat"] = -1
#
#for index in range(0,9):
#    print("")
#    print(index)
#    cont = 0
#    for line in open("../rawdata/SET2TSV/POS_SAMPLE_%d.TSV" % (index) , 'r'):
#        line = line.replace("\n","")
#        chunks = line.split("\t")
#        if len(chunks) == 3:
#            cont += 1
#            traces.insert({"userid" : int(chunks[0]), 
#                            "date": datetime.strptime(chunks[1], '%Y-%m-%d %H:%M:%S'), 
#                            "antennaid": int(chunks[2]),
#                            "antenna" : antennas[int(chunks[2])]})
#    print("cont = ",cont)
    
    
    
# DATASET 3 - SUB-PREFECTURES
    
# Get collection
traces_by_subprefectures = __get_collection(config, "traces_by_subprefectures")

#  Insert subprefectures id into a hash to get lon/lat
subprefectures = {}
for line in open("../../d4d-datasets/SUBPREF_POS_LONLAT.TSV" , 'r'):
    line = line.replace("\n","")
    chunks = line.split("\t")
    if len(chunks) == 3:
        subprefecture_id = int(chunks[0])
        subprefectures[subprefecture_id] = {}
        subprefectures[subprefecture_id]["lon"] = float(chunks[1])
        subprefectures[subprefecture_id]["lat"] = float(chunks[2])
subprefectures[-1] = {}
subprefectures[-1]["lon"] = -1
subprefectures[-1]["lat"] = -1

for index in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'):
    print("")
    print(index)
    cont = 0
    for line in open("../../d4d-datasets/SUBPREF_POS_SAMPLE_%s.TSV" % (index) , 'r'):
        line = line.replace("\n","")
        chunks = line.split("\t")
        if len(chunks) == 3:
            cont += 1
            traces_by_subprefectures.insert({"userid" : int(chunks[0]), 
                            "date": datetime.strptime(chunks[1], '%Y-%m-%d %H:%M:%S'), 
                            "subprefectureid": int(chunks[2]),
                            "subprefecture" : subprefectures[int(chunks[2])]})
    print("cont = ",cont)
