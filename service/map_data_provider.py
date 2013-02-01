# -*- coding: utf-8 -*-
import logging
import traceback
from collections import defaultdict

days = ['L', 'M', 'X', 'J', 'V', 'S', 'D']
hours = range(24)

logging.basicConfig(format='[%(levelname)s][%(asctime)s] %(message)s',level=logging.INFO)

data = defaultdict(lambda : defaultdict(dict))

for day in days:
    for hour in hours:
        logging.info("Storing data in memory for [%s][%s]" % (day, hour))
        try:
            with(open('../antennas_weight/%s_hour_%s.tsv' % (day, hour),'r')) as data_file:
                result = {
                            'type': 'FeatureCollection',
                            'features': []
                         }
                for line in data_file.readlines():
                    line = line.replace('\n', '')
                    fields = line.split('\t')
                    try:
                        feature = {"type": "Feature",
                                   "properties": {"weight": float(fields[2])},
                                   "geometry": {
                                        "type": "Point",
                                        "coordinates": [float(fields[0]), float(fields[1])]
                                   }
                                  }
                        result['features'].append(feature)
                    except ValueError as e:
                        pass
                data[day][str(hour)] = result
        except Exception as e:
            raise e
            logging.error("The file %s_hour_%s.tsv does not exist" % (day, hour))

def get_data(day, hour):
    print(data.keys())
    return data.get(day, {}).get(hour, {})

