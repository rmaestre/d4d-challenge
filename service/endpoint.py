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
import tarfile
import uuid
import shutil
import traceback

    
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
        try:
            self.assert_date_format(date_start)
            self.assert_date_format(date_end)
            assert(output in ["shp", "dot"])

            # Call to SpaceTemporalModel manager
            stm = SpaceTemporalModel()
            # "2011:07:12:00", "2011:07:12:12"
            traces = stm.retieve_data_and_create_model(date_start, date_end)
            lines = []
            antennas = {}
            for trace in traces:
                if len(traces[trace]) > 0:
                    line = []
                    for point in traces[trace]["trace"]:
                        line.append([point[1],point[0]])
                        if (point[1],point[0]) not in antennas:
                            antennas[(point[1],point[0])] = 1
                        else:
                            antennas[(point[1],point[0])] += 1
                    lines.append(line)

            # create unique temporal id
            tmp_id = str(uuid.uuid1())
            # Save lines to SHP
            w_traces = shapefile.Writer(shapefile.POLYLINE)
            w_traces.field('FIRST_FLD','C','40')
            for line in lines:
                if len(line) > 1:
                    w_traces.line(parts = [line])
                    w_traces.record(FIRST_FLD='First')
                else:
                    print("Line jumped")
            
        
            # Save antenna points
            w = shapefile.Writer(shapefile.POINT)
            w.field('FIRST_FLD')
            w.field('SECOND_FLD','C','40')
            for antenna in antennas:
                w.point(antenna[0], antenna[1])
                w.record(FIRST_FLD='First')
        
            try:
                w_traces.save('/tmp/%s/commuting_polylines.shp' % tmp_id)
                w.save('/tmp/%s/commuting_antennas.shp' % tmp_id)

                tar = tarfile.open("/tmp/%s/data.tar.gz" % tmp_id, "w:gz")
                for name in ['/tmp/%s/commuting_antennas.shp' % tmp_id, 
                            '/tmp/%s/commuting_antennas.dbf' % tmp_id,
                            '/tmp/%s/commuting_antennas.shx' % tmp_id,
                            '/tmp/%s/commuting_polylines.shp' % tmp_id,
                            '/tmp/%s/commuting_polylines.dbf' % tmp_id,
                            '/tmp/%s/commuting_polylines.shx' % tmp_id]:
                            tar.add(name)
                tar.close()

                self.set_header("Content-Disposition","attachment;filename=data_%s.tar.gz" % tmp_id);
                File = open("/tmp/%s/data.tar.gz" % tmp_id,"rb")
                self.write(File.read())
                File.close()
                # Remove temporal dir
                shutil.rmtree("/tmp/%s/" % tmp_id)
            except:
                print(traceback.format_exc())
                self.write("No data between temporal tange")
        except Exception:
            print(traceback.format_exc())
            self.write("Format dates are not correct")

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
   app.listen(80)
   tornado.ioloop.IOLoop.instance().start()






