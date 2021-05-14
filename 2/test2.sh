#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part2/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/my_parking_data.csv /part2/input/
/usr/local/spark/bin/spark-submit \
        --master=spark://$SPARK_MASTER:7077 \
        ./part2.py hdfs://$SPARK_MASTER:9000/part2/input/


