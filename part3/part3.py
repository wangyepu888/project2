#!/usr/bin/python
# --*-- coding:utf-8 --*--

from __future__ import print_function

import sys
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("Part3")\
        .getOrCreate()

    lines = spark.read.format("csv").option("header", "true").load(sys.argv[1]).rdd.map(lambda r: r[19])
    counts = lines.map(lambda x: (x, 1)) \
                  .reduceByKey(add) \
                  .sortBy(lambda a: a[1],ascending=False)
                  
    
    
    output = counts.collect()
    for (time, count) in output[:1]:
        print("%s: %i" % (time, count))

   
    spark.stop()