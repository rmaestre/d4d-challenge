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
        Init method
        """
        # Nothing to-do

        
    def retieve_data_and_create_model(self, strech):
        """
        Prerequisite: The date format should be: (hour from , hour to)
        """
        # Debug prints
        users = {}
        graph = {}
        for index in range(0,9):
            print("")
            print(index)
            cont = 0
            for line in open("../rawdata/SET2TSV/POS_SAMPLE_%d.TSV" % (index) , 'r'):
                line = line.replace("\n","")
                chunks = line.split("\t")
                if len(chunks) == 3:
                    item = {}
                    item["userid"] = int(chunks[0])
                    item["antennaid"] = int(chunks[2])
                    item["date"] = datetime.strptime(chunks[1], '%Y-%m-%d %H:%M:%S')
                    if (item["date"].hour >= strech[0] and item["date"].hour<= strech[1] and 
                        item["date"].weekday()!=5 and item["date"].weekday()!=6):
 
                        if item['userid'] not in users:
                            users[item['userid']] = {}
                            users[item['userid']]["trace"] = []
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
model = stm.retieve_data_and_create_model((11,15))
print(model)








