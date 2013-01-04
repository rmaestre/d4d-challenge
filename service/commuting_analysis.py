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


data = pickle.load(open( "tmp/dataset2_matrix.pyc", "rb" ) )

for week_day in ['Monday']: #, 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    filename = "tmp/dataset2_matrix_%s.tsv" % week_day
    f_out = open(filename, "w")
    f_out.write("HOURS\tDISTANCES\tTRANSITIONS\tCALLS\tUSERS\tDYNAMIC_USERS\tSTATIC_USERS\tRATIO_DISTANCE2DYNAMIC_USERS\n")
    
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
        
        calls = data[week_day_id][hour]["calls"]
#            calls = [i for i in calls if i != 0]
        mean_calls = sum(calls)/len(calls)
    
        users = data[week_day_id][hour]["users"]
#            users = [i for i in users if i != 0]
        mean_users = sum(users)/len(users)
        
        dynamic_users = data[week_day_id][hour]["dynamic_users"]
#            dynamic_users = [i for i in dynamic_users if i != 0]
        mean_dynamic_users = sum(dynamic_users)/len(dynamic_users)
        
        static_users = data[week_day_id][hour]["static_users"]
#            static_users = [i for i in static_users if i != 0]
        mean_static_users = sum(static_users)/len(static_users)

        # NORMALIZED MAGNITUDES
        ratio_distances_dynamic_users = []
        i = 0
        while i < len(distances):
            if dynamic_users[i] != 0:
                ratio_distances_dynamic_users.append(distances[i]/dynamic_users[i])
            i += 1
        mean_ratio_distances_dynamic_users = sum(ratio_distances_dynamic_users)/len(ratio_distances_dynamic_users)
        
        
#        # deviation
#        n = len(distances)
#        acum = 0
#        i = 0
#        while i < n:
#             acum += math.pow(distances[i] - mean_distances,2)
#             i += 1
#        deviation = math.sqrt(acum * (1/(n-1)))
        
        f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s" % (mean_distances, mean_transitions, mean_calls, mean_users, mean_dynamic_users, mean_static_users, mean_ratio_distances_dynamic_users))
        f_out.write("\n")
    f_out.close()        
        
        
        
        