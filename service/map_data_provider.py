# -*- coding: utf-8 -*-
import logging
import traceback
from collections import defaultdict

days = ['L', 'M', 'X', 'J', 'V', 'S', 'D']
hours = range(24)

logging.basicConfig(format='[%(levelname)s][%(asctime)s] %(message)s',level=logging.INFO)

data = defaultdict(lambda : defaultdict(list))

for day in days:
    for hour in hours:
        logging.info("Storing data in memory for [%s][%s]" % (day, hour))
        try:
            with(open('%s_hour_%s.tsv' % (day, hour),'r')) as data_file:
                for line in data_file.readlines():
                    line = line.replace('\n', '')
                    fields = line.split('\t')
                    point = {'coordinates':[float(fields[0]), float(fields[1])], 
                             'weight': float(fields[2])}
                    data[day][str(hour)].append(point)
        except ValueError as e:
            pass
        except Exception as e:
            logging.error("The file %s_hour_%s.tsv does not exist" % (day, hour))

def get_data(day, hour):
    print(data.keys())
    print(data['L'].keys())
    return data.get(day, {}).get(hour, {})
