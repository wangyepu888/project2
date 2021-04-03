#!/bin/sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /count/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /count/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /count/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../mapreduce-test-data/shot_logs.csv/count/input/
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file ../../mapreduce-test-python/count/mapper.py -mapper ../../mapreduce-test-python/count/mapper.py \
-file ../../mapreduce-test-python/count/reducer.py -reducer ../../mapreduce-test-python/count/reducer.py \
-input /count/input/* -output /count/output/
/usr/local/hadoop/bin/hdfs dfs -cat /count/output/part-00000
/usr/local/hadoop/bin/hdfs dfs -rm -r /count/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /count/output/
../../stop.sh
