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

# Get collection
traces = __get_collection(config, "traces")

#  Insert antennas id into a hash to get lon/lat
antennas = {}
for line in open("../rawdata/ANT_POS.TSV" , 'r'):
    line = line.replace("\n","")
    chunks = line.split("\t")
    if len(chunks) == 3:
        antenna_id = int(chunks[0])
        antennas[antenna_id] = {}
        antennas[antenna_id]["lon"] = float(chunks[1])
        antennas[antenna_id]["lat"] = float(chunks[2])
# insert default antenna
antennas[-1] = {}
antennas[-1]["lon"] = -1
antennas[-1]["lat"] = -1

for index in range(0,9):
    print("")
    print(index)
    cont = 0
    for line in open("../rawdata/SET2TSV/POS_SAMPLE_%d.TSV" % (index) , 'r'):
        line = line.replace("\n","")
        chunks = line.split("\t")
        if len(chunks) == 3:
            cont += 1
            traces.insert({"userid" : int(chunks[0]), 
                            "date": datetime.strptime(chunks[1], '%Y-%m-%d %H:%M:%S'), 
                            "antennaid": int(chunks[2]),
                            "antenna" : antennas[int(chunks[2])]})
    print("cont = ",cont)