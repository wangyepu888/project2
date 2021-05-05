#!/usr/bin/python
# --*-- coding:utf-8 --*--
from __future__ import print_function

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext
from pyspark.sql import Window
from pyspark import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.functions import regexp_replace


import sys

if __name__ == "__main__":
    sc = SparkContext(appName="Part1")
    sqlContext = SQLContext(sc)
    df=sqlContext.read.format("csv").option("header","true").load(sys.argv[1])\
            .select("player_name","SHOT_DIST","CLOSE_DEF_DIST","SHOT_CLOCK","SHOT_RESULT")
    
    df=df.na.drop() 
    df = df.withColumn('SHOT_RESULT', regexp_replace('SHOT_RESULT', 'missed', '0'))
    df = df.withColumn('SHOT_RESULT', regexp_replace('SHOT_RESULT', 'made', '1'))
    FEATURES_COL = ["SHOT_DIST","CLOSE_DEF_DIST","SHOT_CLOCK"]
   
    for col in df.columns:
        if col in FEATURES_COL:
            df = df.withColumn(col,df[col].cast('float'))

    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_kmeans = vecAssembler.transform(df).select('player_name', 'features','SHOT_RESULT')
    df_kmeans = df_kmeans.withColumn('SHOT_RESULT',df_kmeans['SHOT_RESULT'].cast('float'))
    

    kmeans = KMeans().setK(4).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(df_kmeans)
    centers = model.clusterCenters()

    print("Cluster Centers: ")
    for center in centers:
        print(center)
    
    players = ['james harden', 'chris paul', 'stephen curry', 'lebron james']
    df_kmeans=df_kmeans[df_kmeans['player_name'].isin(players)]
    
    transformed = model.transform(df_kmeans).select('player_name', 'prediction','SHOT_RESULT')
    rows = transformed.collect()
    df_pred = sqlContext.createDataFrame(rows)
   
    mydf=df_pred.groupBy("player_name","prediction").mean("SHOT_RESULT")\
           .sort("player_name","prediction")
    mydf.show()
    
    
    w=Window.partitionBy("player_name")
    df_1=mydf.withColumn('maxavg',f.max('avg(SHOT_RESULT)').over(w))\
       .where(f.col('avg(SHOT_RESULT)')==f.col('maxavg'))\
       .drop('maxavg')\
       .show()
                
    
    sc.stop()

