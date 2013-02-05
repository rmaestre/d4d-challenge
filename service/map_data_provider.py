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

def get_data(start_day, end_day, start_hour, end_hour):
    result = defaultdict(dict)
    if start_day == end_day:
        for hour in range(start_hour, end_hour + 1):
            result[start_day][hour] = data.get(start_day, {}).get(str(hour), {})
    else:
        days = __build_day_range(start_day, end_day)
        print(days)
        for day in days:
            if day == start_day:
                for hour in range(start_hour, 24):
                    result[day][hour] = data.get(day, {}).get(str(hour), {})
            elif day == end_day:
                for hour in range(0, end_hour + 1):
                    result[day][hour] = data.get(day, {}).get(str(hour), {})
            else:
                for hour in range(0, 24):
                    result[day][hour] = data.get(day, {}).get(str(hour), {})
    return result

def __build_day_range(start_day, end_day):
    result = []
    for day in days:
        if day == start_day and not result:
            result.append(day)
        if day == end_day and result:
            if not day in result:
                result.append(day)
            break
        elif result and not day in result:
            result.append(day)
    return result


