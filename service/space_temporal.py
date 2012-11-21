#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class provides a interface to model a range of data
    into a matrix-st model
    
    Created 21/11/2012
    
    @author: Paradigma Labs
"""
from pymongo import *
import datetime

class SpaceTemporalModel:
    """
    """
    def __init__(self):
        """
        Load and retieve collection pointer from MongoDB
        """
        self.config = {}
        self.config['db'] = {}
        self.config['db']['host'] = "web40"
        self.config['db']['port'] = 27017
        self.config['db']['db'] = "d4dchallenge"
        self.config['db']['collection'] = "traces"
        self.traces = self.__get_collection(self.config)

    def __get_collection(self, config):
        """
        Return collection from MongoDB with a specific configuration
        """
        connection = Connection(config['db']['host'], config['db']['port'])
        db = connection[config['db']['db']]
        collection = db[config['db']['collection']]
        return collection
        
    def retieve_data_and_create_model(self, date_start, date_end):
        """
        Prerequisite: The date format should be: YYYY:DD:MM:HH, e.g.: 2011:21:12:03
        """
        chunk_ds = date_start.split(":")
        chunk_de = date_end.split(":")
        start = datetime.datetime(int(chunk_ds[0]), int(chunk_ds[2]), int(chunk_ds[1]), int(chunk_ds[3]), 0, 0)
        end = datetime.datetime(int(chunk_de[0]), int(chunk_de[2]), int(chunk_de[1]), int(chunk_de[3]), 0, 0)
        # Debug prints
        print("Extract data from: %s to %s" % (start, end))
        users = {}
        for item in self.traces.find({'date': {'$gte': start, '$lt':end}}):
            if item['userid'] not in users:
                users[item['userid']] = {}
                users[item['userid']]["trace"] = []
            if item['antenna']["lon"] != -1:
                users[item['userid']]["trace"].append((item['antenna']["lat"], item['antenna']["lon"], item['date']))
        return users
        
        
        
"""
stm = SpaceTemporalModel()
stm.retieve_data_and_create_model("2011:07:12:00", "2011:07:12:12")
"""








