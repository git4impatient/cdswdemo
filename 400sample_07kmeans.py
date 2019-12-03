from __future__ import print_function
!echo $PYTHON_PATH
import os, sys
#import path
from pyspark.sql import *

# need a data table
# from hue samples
#  create table sample_07p stored as parquet as select * from sample_07;
# in CDP make it an external table that you can read
# create spark sql session
myspark = SparkSession\
    .builder\
    .config("spark.executor.instances", 3 ) \
    .config("spark.executor.memory", "5g") \
    .config("spark.executor.cores", 2) \
    .config("spark.dynamicAllocation.maxExecutors", 10) \
    .config("spark.scheduler.listenerbus.eventqueue.size", 10000) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .appName("Sample_07_kmeans") \
    .getOrCreate()



sc = myspark.sparkContext

import time
print ( time.time())

sc.setLogLevel("ERROR")
print ( myspark )
# make spark print text instead of octal
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# read in the data file from HDFS
# location of hive warehouse changes in CDP 
dfpfc = myspark.read.parquet ( "/user/hive/warehouse/sample_07p")
# you can also read directly from an s3 bucket, you would of course need the s3 IAM key 
# and permission to read the bucket
#dfpfc = myspark.read.parquet ( "s3a://impalas3a/sample_07_s3a_parquet")
# print number of rows and type of object
print ( dfpfc.count() )
print  ( dfpfc )

# create a table name to use for queries
dfpfc.createOrReplaceTempView("census07")
# run a query
fcout=myspark.sql('select * from census07 where salary > 100000')
fcout.show(5)
# create a dataframe with valid rows
mydf=myspark.sql('select code as txtlabel, salary, total_emp from census07 where total_emp > 0 and total_emp< 1000000 and salary >0 and salary<500000' )
mydf.show(5)

# need to convert from text field to numeric
# this is a common requirement when using sparkML
from pyspark.ml.feature import StringIndexer

# this will convert each unique string into a numeric
indexer = StringIndexer(inputCol="txtlabel", outputCol="label")
indexed = indexer.fit(mydf).transform(mydf)
indexed.show(5)
# now we need to create  a  "label" and "features"
# input for using the sparkML library

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

assembler = VectorAssembler(
    inputCols=[ "total_emp","salary"],
    outputCol="features")
output = assembler.transform(indexed)
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
kmeans = KMeans().setK(10).setSeed(1)
# run the model
model = kmeans.fit(output)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = model.computeCost(output)
print("Within Set Sum of Squared Errors = " + str(wssse))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
# total employees and salary cluster centers 
for center in centers:
    print(center)
    
# now create pretty graph
#%matplotlib inline
import matplotlib.pyplot as plt

def plotit(numpts):
 for row in output.take(numpts):
    plt.scatter(row[2],row[1], color=['blue'])
 for center in centers:
      plt.scatter(center[0],center[1],color=['red'])
 plt.show()
      
plotit(400)

