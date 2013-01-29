#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class provides an endpoint to retrieve precalculated data from 
    d4d commuting calculations.
    
    Created 24/01/2013
    
    @author: Paradigma Labs
"""
from map_data_provider import get_data
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, asynchronous
import logging
import tornado.ioloop

class MapService(tornado.web.RequestHandler):

    def initialize(self):
        pass

    # @tornado.web.asynchronous
    def get(self):
        """
            Returns the geoJSON for the specified day and start and
            end time
        """
        try:
            day = self.get_argument('day')
            start_time = int(self.get_argument('start_time'))
            end_time = int(self.get_argument('end_time'))
            hour_range = range(start_time, end_time + 1)
            result = {}
            for hour in hour_range:
                logging.info('Retrieving data for hour %s' % hour)
                result[str(hour)] = get_data(day, str(hour))
            logging.info(result)
            self.write(result)
            logging.info("Done")
        except Exception as e:
            logging.error(error)
            self.write("There was an error")


logging.basicConfig(format='[%(levelname)s][%(asctime)s] %(message)s',level=logging.INFO)
app = tornado.web.Application([(r"/map", MapService, dict())])

if __name__ == '__main__':
    logging.info("Starting Map Service on port 10001")
    app.listen(10001)
    tornado.ioloop.IOLoop.instance().start()
