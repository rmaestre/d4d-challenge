#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class provides an endpoint to access and generate a flow dynamics
    based on pipelines (edges), forces oppositions (direction) and flows (intensity)
    
    Created 28/11/2012
    
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

    
class FlowDynamicService(tornado.web.RequestHandler):
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
            nodes_index = {}
            edges_density = {}

            for trace in traces:
                if len(traces[trace]) > 0:
                    line = []
                    for point in traces[trace]["trace"]:
                        line.append([point[1],point[0]])
                    i = 0
                    n = len(line)
                    if n > 1:
                        while i < n-1:
                            point_from = (line[i][0],line[i][1])
                            point_to = (line[i+1][0],line[i+1][1])
                            # Create uniqe index of points
                            if point_from not in nodes_index:
                                nodes_index[point_from] = 0
                            if point_to not in nodes_index:
                                nodes_index[point_to] = 0
                            # Crete or add weight to edge
                            if point_from not in edges_density:
                                edges_density[point_from] = {}
                            if point_to not in edges_density[point_from]:
                                edges_density[point_from][point_to] = 1
                            else:
                                edges_density[point_from][point_to] += 1
                            i += 1
                            
            # Final force edges
            final_flow_force = {}
            # Apply flow physical model to get the maximum flow direction
            for node_from in edges_density:
                # Check inverse edge for each edge of current node
                for node_to in edges_density[node_from]:
                    if node_to in edges_density and node_from in edges_density[node_to]:
                        # Self edge to one node
                        if node_from == node_to:
                            if node_from not in final_flow_force:
                                final_flow_force[node_from] = {}
                            final_flow_force[node_from][node_to] = edges_density[node_from][node_to]
                            print("s (%s,%s):%s" % (node_from,node_to,final_flow_force[node_from][node_to]))
                        else:
                            flow_direction_force = edges_density[node_from][node_to] - edges_density[node_to][node_from]
                            # Direct direction win
                            if flow_direction_force > 0:
                                if node_from not in final_flow_force:
                                    final_flow_force[node_from] = {}
                                final_flow_force[node_from][node_to] = flow_direction_force
                                print("+ (%s,%s):%s" % (node_from,node_to,final_flow_force[node_from][node_to]))
                            # Inverse direction win       
                            elif flow_direction_force < 0:
                                if node_to not in final_flow_force:
                                    final_flow_force[node_to] = {}
                                final_flow_force[node_to][node_from] = flow_direction_force * -1
                                print("- (%s,%s):%s" % (node_from,node_to,final_flow_force[node_to][node_from]))
                
                
            # Create a .dot graph
            file_out = file("/tmp/graph.dot", "w")
            for node_from in final_flow_force:
                for node_to in final_flow_force[node_from]:
                    file_out.write()
                    w_traces.line(parts = [[node_from, node_to]])
                
                
            # create unique temporal id
            tmp_id = str(uuid.uuid1())
            # Save lines to SHP
            w_traces = shapefile.Writer(shapefile.POLYLINE)
            w_traces.field('FORCE','C','40')
            for node_from in final_flow_force:
                for node_to in final_flow_force[node_from]:
                    w_traces.line(parts = [[node_from, node_to]])
                    w_traces.record(FORCE = final_flow_force[node_from][node_to])

            try:
                w_traces.save('/tmp/%s/flows.shp' % tmp_id)
                
                tar = tarfile.open("/tmp/%s/data.tar.gz" % tmp_id, "w:gz")
                for name in ['/tmp/%s/flows.shp' % tmp_id,
                            '/tmp/%s/flows.dbf' % tmp_id,
                            '/tmp/%s/flows.shx' % tmp_id]:
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
    (r"/flow_dynamic", FlowDynamicService, dict())])

    
# To test single server file
if __name__ == '__main__':
   app.listen(8080)
   tornado.ioloop.IOLoop.instance().start()






