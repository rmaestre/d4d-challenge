#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    This class provides an endpoint to access and generate several outputs
    from D4D Challenge data
    
    Created 21/11/2012
    
    @author: Paradigma Labs
"""
import datetime
from space_temporal import SpaceTemporalModel
import shapefile

def minutes(td):
    return (td.seconds // 60) % 60

stm = SpaceTemporalModel()
day = "18"
month = "03"


f_out = open("/tmp/data.tsv", "w")
f_out.write("hour\tindex\n")

for i in range(0,23,1):
    
    traces = stm.retieve_data_and_create_model("2012:%s:%s:%s"%(day,month, i), 
                                                "2012:%s:%s:%s"%(day,month, i+1))

    users_means = {}
    number_calls = 0
    for trace in traces:
        if trace not in users_means:
            users_means[trace] = []
        data = []
        if len(traces[trace]["trace"]) > 1:
            number_calls += len(traces[trace]["trace"])
            number_connections = 0
            prev_trace = (traces[trace]["trace"][0][0],traces[trace]["trace"][1][1])
            prev_date = traces[trace]["trace"][0][2]
            for user_trace in traces[trace]["trace"]:
                # Antenna repetead
                if (user_trace[0],user_trace[1]) == prev_trace:
                    number_connections += 1
                # Antenna is changed
                else:
                    # If we have more than two conecctions
                    if number_connections > 1:
                        #users_means[trace].append(minutes(user_trace[2] - prev_date))
                        users_means[trace].append(1)
                    # Update values to for loop
                    prev_trace = (user_trace[0],user_trace[1])
                    prev_date = user_trace[2]
                    number_connections = 0
            # Decrement the last loop counter
            number_connections -= 1
            if number_connections > 1:
                #users_means[trace].append(minutes(user_trace[2] - prev_date))
                users_means[trace].append(1)
    n_sum = 0
    n_cont = 0
    cont = 0
    for user_mean in users_means:
        if len(users_means[user_mean]) > 0:
            #print(user_mean,": ",users_means[user_mean])
            n_sum += sum(users_means[user_mean])
            n_cont += len(users_means[user_mean])
            
    f_out.write("%s\t%s\n" % (number_calls, n_sum))
    print("Commuting index %s" % (n_sum))

f_out.close()
        
        