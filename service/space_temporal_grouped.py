#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class perform a whole iteration over data filtering traces
    by day of the week (no Sundays and Saturdays) and by range of hours.
    It will be grouped by user traces.
    
    Created 03/12/2012
    
    @author: Paradigma Labs
"""
from pymongo import *
from datetime import datetime
from datetime import timedelta
import pickle


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
        
    def retieve_data_and_create_model(self, strech):
        """
        Prerequisite: The date format should be: (hour from , hour to)
        """
        # Debug prints
        users = {}
        graph = {}
        for item in self.traces.find():
            if (item["date"].hour >= strech[0] and item["date"].hour<= strech[1] and 
                item["date"].weekday()!=5 and item["date"].weekday()!=6):
                if item['userid'] not in users:
                    users[item['userid']] = {}
                    users[item['userid']]["trace"] = []
                if item['antenna']["lon"] != -1:
                    if len(users[item['userid']]["trace"]) == 0:
                        users[item['userid']]["trace"].append(item["antennaid"])
                    else:
                        if item["antennaid"] not in graph:
                            graph[item["antennaid"]] = {}
                        if users[item['userid']]["trace"][0] not in graph[item["antennaid"]]:
                            graph[item["antennaid"]][users[item['userid']]["trace"][0]] = 1
                        else:
                            graph[item["antennaid"]][users[item['userid']]["trace"][0]] += 1
                        # drop list and save las point
                        users[item['userid']]["trace"] = [item["antennaid"]]
        pickle.dump(graph, open( "graph_grouped%s-%s.pkl" % (strech[0],strech[1]), "wb" ) )
        
        
stm = SpaceTemporalModel()
model = stm.retieve_data_and_create_model((6,9))
print(model)








