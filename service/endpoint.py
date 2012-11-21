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
        # format: DD:MM:HH, e.g.: 21:12:03
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
        
        
        
        
        # TODO
        
        # Conexion a mongo con el filtrado por fechas (los datos estarán en la mongo de web40)
        
        # Pasarle los datos a la clase creada por BOB para que genere la matriz spacio-temporal
        
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
        # Three parts day+month+year 
        assert(len(chunks) == 3) 
        # Each part with len == 2
        assert(len(chunks[0]) == 2 and len(chunks[1]) == 2 and len(chunks[2]) == 2)
        # Only digits are allowed
        assert(chunks[0].isdigit() and chunks[1].isdigit() and chunks[2].isdigit())
        # Ranges for days, months and years
        assert(1<=int(chunks[0])<=31 and 1<=int(chunks[1])<=12 and 1<=int(chunks[2])<=99)
                
                
                
                
# run application
app = tornado.web.Application([
    (r"/ner", EndpointService, dict())])

    
# To test single server file
if __name__ == '__main__':
   app.listen(8008)
   tornado.ioloop.IOLoop.instance().start()






