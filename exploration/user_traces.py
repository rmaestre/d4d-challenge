
fd_out = open("/tmp/user_lonlat_distributions.tsv", "w")
fd_out.write("iduser\tall\tunique\n")

users = {}
for index in range(0,1):
    for line in open("../rawdata/SET2TSV/POS_SAMPLE_%d.TSV" % (index) , 'r'):
        line = line.replace("\n","")
        chunks = line.split("\t")
        if len(chunks) == 3:
            if chunks[0] not in users:
                users[chunks[0]] = []
            users[chunks[0]].append(chunks[2])
            
    for user in users:
        ant = users[user][0]
        cont = 1
        for point in users[user]:
            if point != ant:
                ant = point
                cont += 1
        print(users[user])
        if cont > 0 and cont < 50:
            user_hash = "%s\t%d\t%s\n" % (user, len(users[user]), cont)
            print(user_hash)
            fd_out.write(user_hash)
        