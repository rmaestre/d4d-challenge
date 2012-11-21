#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class provides an endpoint to access and generate several outputs
    from D4D Challenge data
    
    Created 21/11/2012
    
    @author: Paradigma Labs
"""
import tornado.ioloop
from tornado.web import Application, RequestHandler, asynchronous
from tornado.ioloop import IOLoop
import datetime
import time   
import urllib
import json
import pickle
import operator
import re
import os, sys
from types import *
from pymongo import *
from space_temporal import SpaceTemporalModel
import shapefile
#sys.path.append("./utils")

    
class EndpointService(tornado.web.RequestHandler):
    """
    Documentation here
    """
    def initialize(self):
        """
        """
        # Import category model
        # nothing todo
    def get(self):
        """
        """
        # Get parameters. Dates range.
        # format: YYYY:DD:MM:HH, e.g.: 2011:21:12:03
        date_start = self.get_argument("ds")
        date_end = self.get_argument("de")
        output = self.get_argument("output")
        # Starting script benchmark
        a = time.time()
        # Prepare output data structure
        response = {}
        response["result"] = {}
        
        # Check input format constrains
        self.assert_date_format(date_start)
        self.assert_date_format(date_end)
        assert(output in ["shp", "dot"])
        
        
        # Create shapefile output structure
        w = shapefile.Writer(shapefile.POLYLINE)
        # Call to SpaceTemporalModel manager
        stm = SpaceTemporalModel()
        # "2011:07:12:00", "2011:07:12:12"
        traces = stm.retieve_data_and_create_model(date_start, date_end)
        lines = []
        for trace in traces:
            if len(traces[trace]) > 0:
                line = []
                for point in traces[trace]["trace"]:
                    line.append([point[1],point[0]])
                lines.append(line)

        # Save lines to SHP
        w.line(parts=lines)
        w.field('FIRST_FLD','C','40')
        w.field('SECOND_FLD','C','40')
        w.record('First','Polygon')
        w.save('geodata/polygon.shp')
            
        # Segun el OUTPUT y con la matriz spacio-temporal, generar el fichero de salida
        
        
        
        response["result"] = None
        response["time"] = time.time() - a
        

        self.write(response)
    
    
    
    
    def assert_date_format(self, date_target):
        """
        Checking the input date format using an "upto" asserts
        """
        # Split day:month:year
        chunks = date_target.split(":")
        # Checking asserts
        # Three parts day+month+hour+year 
        assert(len(chunks) == 4) 
        # Each part with len == 2
        assert(len(chunks[0]) == 4 and len(chunks[1]) == 2 and len(chunks[2]) == 2 and len(chunks[3]) == 2)
        # Only digits are allowed
        assert(chunks[0].isdigit() and chunks[1].isdigit() and chunks[2].isdigit() and chunks[3].isdigit())
        # Ranges for days, months and years
        assert(2011<=int(chunks[0])<=2012 and 1<=int(chunks[1])<=31 and 1<=int(chunks[2])<=12 and 0<=int(chunks[3])<=24)
                
                
                
                
# run application
app = tornado.web.Application([
    (r"/ner", EndpointService, dict())])

    
# To test single server file
if __name__ == '__main__':
   app.listen(8008)
   tornado.ioloop.IOLoop.instance().start()






