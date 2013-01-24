#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class provides an endpoint to retrieve precalculated data from 
    d4d commuting calculations.
    
    Created 24/01/2012
    
    @author: Paradigma Labs
"""
import tornado.ioloop
from tornado.web import Application, RequestHandler, asynchronous
from tornado.ioloop import IOLoop
import logging

class MapService(tornado.web.RequestHandler):

    def initialize(self):
        """
            Loads JSON precalculated data in memory
        """
        pass

    def get(self):
        """
            Returns the geoJSON for the specified day and start and
            end time
        """
        day = self.get_argument('day')
        start_time = self.get_argument('start_time')
        end_time = self.get_argument('end_time')
        self.write({"day":day, "start_time":start_time, "end_time":end_time})


logging.basicConfig(format='[%(levelname)s][%(asctime)s] %(message)s',level=logging.INFO)
app = tornado.web.Application([(r"/map", MapService, dict())])

if __name__ == '__main__':
    logging.info("Starting Map Service on port 10001")
    app.listen(10001)
    tornado.ioloop.IOLoop.instance().start()
