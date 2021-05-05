from __future__ import print_function
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,count,col,sum
import sys
import numpy as np

def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex

if __name__ == "__main__":
        if len(sys.argv) != 2:
                print("Usage: kmeans <file>",file = sys.stderr)
                sys.exit(-1)

        spark = SparkSession\
        .builder\
        .appName("Part2")\
        .getOrCreate()


        data = spark.read.format("csv").option("header","true").load(sys.argv[1])\
                .select('Street Code1','Street Code2','Street Code3','Vehicle Color')

        data = data.select(data['Street Code1'].cast('float'),data['Street Code2'].cast('float'),\
                data['Street Code3'].cast('float'),data['Vehicle Color'])

        vecAssembler = VectorAssembler(inputCols = ["Street Code1","Street Code2","Street Code3"],\
                outputCol = "features")
        data = vecAssembler.transform(data)

        kmeans = KMeans(k=5,seed=1)
        model = kmeans.fit(data.select('features'))
        result = model.transform(data).cache()
        print(result.show())

        BLK_result = result.groupBy('prediction').agg(\
                count(when(col('Vehicle Color') == 'BLK',1)).alias('Count'), \
                count('Vehicle Color').alias('Total_Cars')).orderBy('prediction')

        Prob = BLK_result.select('prediction','Count', 'Total_Cars', \
                (col('Count') / col('Total_Cars')).alias('Probability'))
        print(Prob.show())

        centers = np.array(model.clusterCenters()).astype(float)
        print("The centers for each clutser is:")
        print(centers)
        Clusterid = closestPoint([34510.0,10030.0,34050.0],centers)
        print('Cluster id for Street Code (34510, 10030, 34050) is: ')
        print(Clusterid)
        print(Prob.filter(col('prediction') == Clusterid).show())


        spark.stop()
