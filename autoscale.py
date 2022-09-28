from __future__ import print_function
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

# ask for large session to force autoscale

spark = SparkSession\
    .builder\
    .appName("forceautoscale")\
    .config("spark.authenticate", "true") \
    .config ("spark.executor.instances", 10) \
    .config("spark.executor.memory ", "18g") \
    .config("spark.executor.cores", 4 ) \
    .config("spark.scheduler.listenerbus.eventqueue.size",  10000) \
    .getOrCreate()
sc = spark.sparkContext
