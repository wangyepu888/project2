#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /census1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /census2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /census/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /census1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /census2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/train.csv /census1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.csv /census2/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part3.py \
            hdfs://$SPARK_MASTER:9000/census1/input/ \
            hdfs://$SPARK_MASTER:9000/census2/input/ \