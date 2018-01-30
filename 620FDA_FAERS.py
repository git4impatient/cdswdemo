#
# copyright 2018 Martin Lurie - all rights reserved - for non-commercial use
#
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
    .appName("FDA_FAERS_logisticRegression") \
    .getOrCreate()



sc = myspark.sparkContext

import time
print ( time.time())

sc.setLogLevel("ERROR")
print ( myspark )
# make spark print text instead of octal
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# read in the data file from HDFS
demo = myspark.read.parquet ( "/user/hive/warehouse/medeventsp")
# print number of rows and type of object
print ( demo.count() )
demo.show(5) 

# create a table name to use for sparkSQL queries
demo.createOrReplaceTempView("faersdemo")
# run a query
faersagewt=myspark.sql('select age, wt*2.2,sex from faersdemo limit 1000')

# now create pretty graph
#%matplotlib inline
import matplotlib.pyplot as plt

# use a function so we can call it with
# varying number of points
def plotit(numpts):
 for row in faersagewt.take(numpts):
    plt.scatter(row[0],row[1], color=['blue'])
 plt.show()
      
plotit(400)

# pairplot to see what we have...
import seaborn as sns
import pandas

outcome= myspark.sql('select casecount, mywt, myage, csex, label from logisticoutcomep')
outcome.show(3)
# seaborn wants a pandas dataframe, not a spark dataframe
# so convert
pdsoutcome = outcome.toPandas()
from IPython.display import display
#display(pdsoutcome)
pdsoutcome.dtypes

sns.set(style="ticks" , color_codes=True)
# this takes a long time to run:  
# you can see it if you uncomment it
g = sns.pairplot(pdsoutcome,  hue="label" )



# now we need to create  a  "label" and "features"
# input for using the sparkML library

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

assembler = VectorAssembler(
    inputCols=[ "casecount", "mywt", "myage", "csex"],
    outputCol="features")
outcomevector = assembler.transform(outcome)

outcomevector.show(2)

# we want to do some data science so split into train and test
(train_df, test_df) = outcomevector.randomSplit([0.7, 0.3], seed=1)
train_df.show(2)
test_df.show(2)






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
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(train_df)

# Print the coefficients and intercept for multinomial logistic regression
print("Coefficients: \n" + str(lrModel.coefficientMatrix))
print("Intercept: " + str(lrModel.interceptVector))


# Extract the summary from the returned LogisticRegressionModel instance trained
# in the earlier example
trainingSummary = lrModel.summary

# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
        print(objective)

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# Set the model threshold to maximize F-Measure
#fMeasure = trainingSummary.fMeasureByThreshold
#maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
#bestThreshold = fMeasure.where(fMeasure['F-Measure'] == maxFMeasure['max(F-Measure)']).select('threshold').head()['threshold']
#lr.setThreshold(bestThreshold)

# compare and test

# Fit the model
lrModel = lr.fit(test_df)

# Print the coefficients and intercept for multinomial logistic regression
print("Coefficients: \n" + str(lrModel.coefficientMatrix))
print("Intercept: " + str(lrModel.interceptVector))


# Extract the summary from the returned LogisticRegressionModel instance trained
# in the earlier example
trainingSummary = lrModel.summary

# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
        print(objective)

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))
