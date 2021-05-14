#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/shot_logs.csv /part1/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part1.py hdfs://$SPARK_MASTER:9000/part1/input/