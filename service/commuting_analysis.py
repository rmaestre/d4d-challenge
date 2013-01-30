import pickle
import math

"""
    Whole date/time range for the 2nd dataset: [1-DIC-2011 -> 28-APR-2012]: 150 days (3600h and 100h missing)
    
    How many ?:
    ===========
    Mondays : 21
    Tuesdays: 21
    Wednesdays: 21
    Thursdays: 22
    Fridays: 22
    Saturdays: 22
    Sundays: 21


"""
from dis import dis

def get_week_day_id(week_day):
    if week_day == 'Monday':
        return 0
    elif week_day == 'Tuesday':
        return 1
    elif week_day == 'Wednesday':
        return 2
    elif week_day == 'Thursday':
        return 3
    elif week_day == 'Friday':
        return 4
    elif week_day == 'Saturday':
        return 5
    elif week_day == 'Sunday':
        return 6


data = pickle.load(open( "/tmp/data/domingo/dataset2_matrix.tsv", "rb" ) )

for week_day in ['Sunday']: #, 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    filename = "/tmp/data/domingo/results.tsv"
    f_out = open(filename, "w")
    f_out.write("HOURS\tDISTANCES\tMEDIAN_DISTANCES\tTRANSITIONS\tSTAYS\tCALLS\tLOCATED_EDGES\tUSERS\tDYNAMIC_USERS\tSTATIC_USERS\tRATIO_DISTANCE2DYNAMIC_USERS\tRATIO_MEDIAN_DISTANCE2DYNAMIC_USERS\tRATIO_DYNAMIC_USERS2USERS\tRATIO_STATIC_USERS2USERS\tRATIO_TRANSITIONS2LOCATED_ANTENNAS\tRATIO_STAYS2LOCATED_ANTENNAS\n")
    
    # 0 = Monday ... 6 = Sunday
    week_day_id = get_week_day_id(week_day)
    
    for hour in range(0,24):
        f_out.write("%s\t" % hour);

        # ABSOLUTE MAGNITUDES
        distances = data[week_day_id][hour]["distances"]
#            distances = [i for i in distances if i != 0]
        mean_distances = sum(distances)/len(distances)

        transitions = data[week_day_id][hour]["transitions"]
#            transitions = [i for i in transitions if i != 0]
        mean_transitions = sum(transitions)/len(transitions)
        
        stays = data[week_day_id][hour]["stays"]
#            stays = [i for i in stays if i != 0]
        mean_stays = sum(stays)/len(stays)
        
        calls = data[week_day_id][hour]["calls"]
#            calls = [i for i in calls if i != 0]
        mean_calls = sum(calls)/len(calls)
    
        located_edges = data[week_day_id][hour]["located_edges"]
#            located_edges = [i for i in located_edges if i != 0]
        mean_located_edges = sum(located_edges)/len(located_edges)
        
        users = data[week_day_id][hour]["users"]
#            users = [i for i in users if i != 0]
        mean_users = sum(users)/len(users)
        
        dynamic_users = data[week_day_id][hour]["dynamic_users"]
#            dynamic_users = [i for i in dynamic_users if i != 0]
        mean_dynamic_users = sum(dynamic_users)/len(dynamic_users)
        
        static_users = data[week_day_id][hour]["static_users"]
#            static_users = [i for i in static_users if i != 0]
        mean_static_users = sum(static_users)/len(static_users)


        # Median calculation
        # Sort values
        distances = data[week_day_id][hour]["distances"]
        distances_flatten = [i for i in distances if i != 0]
        distances_flatten.sort()
        # Get index to get median
        n = round((len(distances_flatten)+1) / 2)
        median_distances = distances_flatten[n]
        
        # Normalized distance from dynamic users
        median_ratio_distances_dynamic_users = median_distances/mean_dynamic_users
        
        # NORMALIZED MAGNITUDES
#        ratio_distances_dynamic_users = []
#        i = 0
#        while i < len(distances):
#            if dynamic_users[i] != 0:
#                ratio_distances_dynamic_users.append(distances[i]/dynamic_users[i])
#            else:
#                ratio_distances_dynamic_users.append(0)
#                print("At %d dynamic user for %d Monday is zero" % (hour, dynamic_users[i]))
#            i += 1
#        mean_ratio_distances_dynamic_users = sum(ratio_distances_dynamic_users)/len(ratio_distances_dynamic_users)
        mean_ratio_distances_dynamic_users = mean_distances/mean_dynamic_users
        
#        ratio_dynamic_users_users = []
#        i = 0
#        while i < len(dynamic_users):
#            if users[i] != 0:
#                ratio_dynamic_users_users.append(dynamic_users[i]/users[i])
#            else:
#                ratio_dynamic_users_users.append(0)
#                print("At %d user for %d Monday is zero" % (hour, users[i]))
#            i += 1
#        mean_ratio_dynamic_users_users = sum(ratio_dynamic_users_users)/len(ratio_dynamic_users_users)
        mean_ratio_dynamic_users_users = mean_dynamic_users/mean_users
 
#       ratio_static_users_users = []
#        i = 0
#        while i < len(static_users):
#            if users[i] != 0:
#                ratio_static_users_users.append(static_users[i]/users[i])
#            else:
#                ratio_static_users_users.append(0)
#                print("At %d user for %d Monday is zero" % (hour,users[i]))
#            i += 1
#        mean_ratio_static_users_users = sum(ratio_static_users_users)/len(ratio_static_users_users)
        mean_ratio_static_users_users = mean_static_users/mean_users
        
#        ratio_transitions_located_antennas = []
        mean_ratio_transitions_located_antennas = mean_transitions/mean_located_edges

#        ratio_stays_located_antennas = []
        mean_ratio_stays_located_antennas = mean_stays/mean_located_edges

        
        
        
#        # deviation
#        n = len(distances)
#        acum = 0
#        i = 0
#        while i < n:
#             acum += math.pow(distances[i] - mean_distances,2)
#             i += 1
#        deviation = math.sqrt(acum * (1/(n-1)))
        
        f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % 
                                        (mean_distances, median_distances, mean_transitions, mean_stays, mean_calls, 
                                        mean_located_edges, mean_users, mean_dynamic_users, mean_static_users, 
                                        mean_ratio_distances_dynamic_users, median_ratio_distances_dynamic_users, 
                                        mean_ratio_dynamic_users_users, mean_ratio_static_users_users, 
                                        mean_ratio_transitions_located_antennas, mean_ratio_stays_located_antennas))
        f_out.write("\n")
    f_out.close()        
        
        
        
