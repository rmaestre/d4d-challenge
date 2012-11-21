#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class provides a interface to model a range of data
    into a matrix-st model
    
    Created 21/11/2012
    
    @author: Paradigma Labs
"""
from pymongo import *


class SpaceTemporalModel:
    """
    """
    def __init__(self):
        self.config = {}
        self.config['db'] = {}
        self.config['db']['host'] = "web40"
        self.config['db']['port'] = 27017
        self.config['db']['db'] = "d4dchallenge"
        self.config['db']['collection'] = "traces"
        self.traces = self.__get_collection(self.config)

    def __get_collection(self, config):
        connection = Connection(config['db']['host'], config['db']['port'])
        db = connection[config['db']['db']]
        collection = db[config['db']['collection']]
        return collection
        
    def retieve_data_and_create_model(self, date_start, date_end):
        print("go")
        
stm = SpaceTemporalModel()

stm.retieve_data_and_create_model("07:12:11", "07:12:11")