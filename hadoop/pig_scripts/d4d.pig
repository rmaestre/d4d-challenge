REGISTER /usr/local/pig-0.10.0/myudfs/myudfs-d4d.jar

-- Load data from HDFS

--data = LOAD 'hdfs://hadoop-01:54310/user/hadoop/d4d/datasets/dataset_control.tsv' USING PigStorage('\t') AS (id_user, timestamp, id_sub);
--data = LOAD 'hdfs://hadoop-01:54310/user/hadoop/d4d/datasets/users/SUBPREF_POS_SAMPLE_A.TSV' USING PigStorage('\t') AS (id_user, timestamp, id_sub);
data = LOAD 'hdfs://hadoop-01:54310/user/hadoop/d4d/datasets/users/' USING PigStorage('\t') AS (id_user, timestamp, id_sub);

-- From each user trace, get the week number (0=Monday, ..., 6=Sunday)
data_with_week = FOREACH data GENERATE flatten(udfs.WeekDayDetection((chararray)timestamp)),id_user,id_sub,timestamp;

-- Cogroup trough a unique key:
--              "week_day:day_of_month:hour"($0) and,
--               iduser($1) pairs
week_users_traces = GROUP data_with_week BY ($0,$1);

user_lenghts = FOREACH week_users_traces GENERATE flatten($0),flatten(udfs.UserTraceLenght($1));

user_lenghts_filtered = FILTER user_lenghts BY $2 != null;

STORE user_lenghts_filtered INTO 'hdfs://hadoop-01:54310/user/hadoop/output';