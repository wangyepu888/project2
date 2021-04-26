#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/Parking_Violations.csv /part3/input/
/usr/local/spark/bin/spark-submit --conf spark.default.parallelism=2 \
                                  --master=spark://$SPARK_MASTER:7077 ./part3.py hdfs://$SPARK_MASTER:9000/part3/input/
