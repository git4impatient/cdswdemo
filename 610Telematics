#
# copyright 2017 Martin Lurie - all rights reserved - for non-commercial use
#
# see multi-car directory for data files and scripts
#
#
from __future__ import print_function
!echo $PYTHON_PATH
import os, sys
from pyspark.sql import *

# create spark sql session
myspark = SparkSession\
    .builder\
    .config("spark.executor.instances", 3 ) \
    .config("spark.executor.memory", "5g") \
    .config("spark.executor.cores", 2) \
    .config("spark.dynamicAllocation.maxExecutors", 10) \
    .config("spark.scheduler.listenerbus.eventqueue.size", 10000) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .appName("Telematics") \
    .getOrCreate()



sc = myspark.sparkContext

import time
print ( time.time())

sc.setLogLevel("ERROR")
print ( myspark )
# make spark print text instead of octal
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# read in the data file from HDFS
dfpfc = myspark.read.parquet ( "/user/hive/warehouse/telematics2p")
# print number of rows and type of object
print ( dfpfc.count() )
print  ( dfpfc )

# create a table name to use for queries
dfpfc.createOrReplaceTempView("telematics")
print (dfpfc)
# run a query
fcout=myspark.sql('select vin, engine_rpm_rpm, speed__obd__mph, gps_speed__meters_second, absolute_throttle_position_pct,acceleration_sensor_total__g from telematics where vin > 0 and acceleration_sensor_total__g != 0  limit 500')

# now create pretty graph
#%matplotlib inline
import matplotlib.pyplot as plt

# use a function so we can call it with
# varying number of points
def plotit(numpts):
 for row in fcout.take(numpts):
    plt.scatter(row[1],row[2], color=['blue'])
 plt.show()
      
plotit(400)

# pairplot to see what we have...
import seaborn as sns
import pandas

# discard variables not useful fcout2=myspark.sql('select vin , longitude, latitude, gps_speed__meters_second, absolute_throttle_position_pct, acceleration_sensor_total__g, acceleration_sensor_x_axis__g, acceleration_sensor_y_axis__g, acceleration_sensor_z_axis__g, air_fuel_ratio_measured, engine_rpm_rpm, kilometers_per_litre_instant__kpl, mass_air_flow_rate_gpers, v1,v2, deltav from telematics where vin is not null and speed__obd__mph > 1  limit 100')
fcout2=myspark.sql('select absolute_throttle_position_pct, acceleration_sensor_total__g, acceleration_sensor_x_axis__g, acceleration_sensor_y_axis__g, acceleration_sensor_z_axis__g, engine_rpm_rpm,  mass_air_flow_rate_gpers, v1,v2, deltav as label from telematics where vin is not null and speed__obd__mph > 1  limit 200')
fcout2.show(3)
# seaborn wants a pandas dataframe, not a spark dataframe
# so convert
pdsdf2 = fcout2.toPandas()
#print (fcout2)
#print (pdsdf2)
sns.set(style="ticks" , color_codes=True)
# this takes a long time to run:  
# you can see it if you uncomment it
g = sns.pairplot(pdsdf2,  hue="label" )
# raw data, without shifting to remove negative values
# shows acceleration and deceleration 
pdsdf2.hist(column = 'label')

# we want to do some data science so split into train and test
(train_df, test_df) = fcout2.randomSplit([0.7, 0.3], seed=1)
train_df.show(2)
test_df.show(2)

# now we need to create  a  "label" and "features"
# input for using the sparkML library

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

assembler = VectorAssembler(
    inputCols=[ "engine_rpm_rpm","v1"],
    outputCol="features")
output = assembler.transform(train_df)

output.show(2)

# if the label field is text need to convert
# ours is already a float
#
# need to convert from text field to numeric
# this is a common requirement when using sparkML
# from pyspark.ml.feature import StringIndexer

# this will convert each unique string into a numeric
#indexer = StringIndexer(inputCol="txtlabel", outputCol="label")
#indexed = indexer.fit(mydf).transform(mydf)
#indexed.show(5)



# note the column headers - label and features are keywords
print ( output.show(3) )

# use the kmeans clustering - do not write it yourself :-)
from pyspark.ml.clustering import KMeans
# 
# after some trial and error 30 centers ok
# but this really isnot a good cluster problem
#
kmeans = KMeans().setK(30).setSeed(1)
# run the model
model = kmeans.fit(output)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = model.computeCost(output)
print("Within Set Sum of Squared Errors = " + str(wssse))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
# centers for rpm as predictor of speed 
for center in centers:
    print(center)
    
# now create pretty graph
#%matplotlib inline
import matplotlib.pyplot as plt

def plotit(numpts):
 for row in output.take(numpts):
    plt.scatter(row[5],row[8], color=['blue'])
 for center in centers:
      plt.scatter(center[0],center[1],color=['red'])
 plt.show()
      
plotit(400)

# now try the test dataset withheld
assembler = VectorAssembler(
    inputCols=[ "engine_rpm_rpm","v1"],
    outputCol="features")
outputtest = assembler.transform(test_df)
model = kmeans.fit(outputtest)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wsssetest = model.computeCost(outputtest)
print("Within Set Sum of Squared Errors train = " + str(wssse) + " " + str(wssse/75)) 

print("Within Set Sum of Squared Errors test = " + str(wsssetest) + " "+str(wssse/25))



# decision tree
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator




# make the deltav an integer > 0  and < 100
# why  -10 < deltav < 10?  We know the car cannot accel or decel more than that
# why deltav/3 ?  We are trying to find bad drivers
# define bad driver as falling into the very high or very low acceleration
# bucket.  We do not need a very granular bucket
treedata=myspark.sql('select absolute_throttle_position_pct, acceleration_sensor_total__g, acceleration_sensor_x_axis__g, acceleration_sensor_y_axis__g, acceleration_sensor_z_axis__g, engine_rpm_rpm,  mass_air_flow_rate_gpers, v1,v2, cast( (deltav+.5)/3+11 as int) as label from telematics where vin is not null and speed__obd__mph > 1 and deltav > -10 and deltav < 10 ')
treedata.show(2)



# build a features column with most of the variables in 
# the data set

assembler = VectorAssembler(
    inputCols=[ "absolute_throttle_position_pct", "acceleration_sensor_total__g", "acceleration_sensor_x_axis__g", "acceleration_sensor_y_axis__g", "acceleration_sensor_z_axis__g", "engine_rpm_rpm",  "mass_air_flow_rate_gpers", "v1","v2"],
    outputCol="features")
treeoutput= assembler.transform(treedata)

(treetrain_df, treetest_df) = treeoutput.randomSplit([0.7, 0.3], seed=1)

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
#dt.setMaxBins(10)

model = dt.fit(treetrain_df)
print (model)


# Make predictions.
predictions = model.transform(treetrain_df)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

# now use the test data

model = dt.fit(treetest_df)
print (model)

predictions = model.transform(treetest_df)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

# show the contents of the model
model.toDebugString

# print out some of the predications 
predictions.select("prediction", "label", "features").show(10)

# switch to pandas dataframe for easy plotting
pdtrain = treetrain_df.toPandas()
# show distribution, this has the offset to avoid negative values
# note the compression of values has created holes in the plot
pdtrain.hist(column = 'label')

# how many values in the dataset
pdtrain.count()

# calculate error if we just guessed the most frequent value in the dataset
print ( "dummy model, guess most frequent value error: " + str(1-1700/2796.0))



