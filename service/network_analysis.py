import pickle
import math 

distribution = {}

data = pickle.load(open( "/tmp/networks.p", "rb" ) )
for week in data:
    if week == 0:
        distribution[week] = {}
        for hour in range(0,24):
            distribution[week][hour] = []
            
        for hour in data[week]:
            for trace in data[week][hour]["degree"]:
                n = len(trace)
                if n > 1:
                    prev_value = trace[0]
                    i = 1
                    cont_degree = 0
                    while i < n:
                        if trace[i] ==  prev_value and i != n - 1:
                            cont_degree += 1
                        elif trace[i] == prev_value and i == n - 1:
                            distribution[week][hour].append(cont_degree + 2)
                        elif trace[i] != prev_value:
                            distribution[week][hour].append(cont_degree + 1)
                            cont_degree = 0
                        prev_value = trace[i]
                        i += 1

print("hour\tdegree\tlen")
for week in distribution:
    for hour in range(0,24):
        distribution[week][hour].sort(reverse=True)
        aux = []
        cont = 0
        for e in distribution[week][hour]:
            aux.append(e)
            cont += 1

        sample_sum = sum(aux)
        sample_len = len(aux)
        if sample_len > 0:
            print("%s\t%s\t%s" % (hour, sample_sum, sample_len))
        else:
            print("%s\t%s\t%s" % (hour, 0, 0))