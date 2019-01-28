from __future__ import print_function
!echo $PYTHON_PATH
import os, sys
#import path
from pyspark.sql import *

# create spark sql session
myspark = SparkSession\
    .builder\
    .config("spark.executor.instances", 4 ) \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", 2) \
    .config("spark.dynamicAllocation.maxExecutors", 10) \
    .config("spark.scheduler.listenerbus.eventqueue.size", 10000) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .appName("telco_kmeans") \
    .getOrCreate()



sc = myspark.sparkContext

import time
print ( time.time())

sc.setLogLevel("ERROR")
print ( myspark )
# make spark print text instead of octal
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# read in the data file from HDFS
dfpfc = myspark.read.parquet ( "/user/hive/warehouse/cdranomaly_p")

# print number of rows and type of object
print ( dfpfc.count() )
print  ( dfpfc )

# create a table name to use for queries
dfpfc.createOrReplaceTempView("cdrs")
myspark.sql ("refresh table cdrs")
# run a query
fcout=myspark.sql('select avg(duration ) from cdrs')
fcout.show(5)
# create a dataframe with valid rows
mydf=myspark.sql('select billidnum as label, sourcenm, duration*10 dur, mytimestamp/100 ts, terminationcode*100 tc from cdrs')
#mydf=myspark.sql('select billidnum as label, sourcenm, duration, mytimestamp, terminationcode from cdrs')

mydf.show(5)

# need to convert from text field to numeric
# this is a common requirement when using sparkML
#from pyspark.ml.feature import StringIndexer

# this will convert each unique string into a numeric
#indexer = StringIndexer(inputCol="sourcenm", outputCol="sourcenumint")
#indexed = indexer.fit(mydf).transform(mydf)
#indexed.show(5)
# now we need to create  a  "label" and "features"
# input for using the sparkML library

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

assembler = VectorAssembler(
    #inputCols=[ "duration", "mytimestamp", "terminationcode"],
    inputCols=[ "dur", "ts", "tc"],
    outputCol="features")
output = assembler.transform(mydf)
# note the column headers - label and features are keywords
print ( output.show(3) )

# use the kmeans clustering - do not write it yourself :-)
from pyspark.ml.clustering import KMeans
# try 10 different centers 
#
#
#  we will start with 10 cluster centers 
#  run the model and then come back here, change the 10 to 15
# highlight from this line to the bottom and select "run selected lines"
# it will then see the cluster in the upper right hand corner of the scatter plot
kmeans = KMeans().setK(20).setSeed(1)
# run the model
model = kmeans.fit(output)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = model.computeCost(output)
print("Within Set Sum of Squared Errors = " + str(wssse))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
# we know duration in hundreds, timestamp in thousands and term code 1 to 10
for center in centers:
    print(center)
    
# now create pretty graph
#%matplotlib inline
import matplotlib.pyplot as plt


#=========================================
# y= duration vs x= timestamp
#print (output.take(3))
def plotit(numpts):
 for row in output.take(numpts):
    plt.scatter(row[3],row[2], color=['blue'])
 for center in centers:
    plt.scatter(center[1],center[0],color=['red'])
 plt.show()
      
plotit(400)

# y= termination code vs x=duration 
#print (output.take(10))
def plotit(numpts):
 for row in output.take(numpts):
    plt.scatter(row[2],row[4], color=['blue'])
 for center in centers:
  plt.scatter(center[0],center[2],color=['red'])
 plt.show()
      
plotit(400)

# compute distance from each point to the center it was assigned
# the anomalies are the points that are the greatest distance
# from the assigned cluster center

# score the data

df_pred = model.transform(output)
df_pred.show(10)
# distance is sqrt (   (x1-x2 )^2 + (y1-y2)^2 + (z1-z2)^2 ) 




