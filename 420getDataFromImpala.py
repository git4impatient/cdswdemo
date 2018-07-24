#
#
# SORRY - THIS EXAMPLE IS BROKEN
# it is on my to-do list to fix 
#
# reset stuff                                               
#!pip uninstall -y sasl
#!pip uninstall -y impyla
#!pip uninstall  -y thrift
#!pip uninstall -y thrift_sasl/

# set up packages we need
!git clone https://github.com/cloudera/thrift_sasl
!pip install -e thrift_sasl/
!pip install thrift==0.9.3
!pip install impyla
!pip show thrift
# some strange overwriting, will be fixed with thrift at 0.10 shortly
!git clone https://github.com/cloudera/impyla.git
!pip install -e impyla/
!pip uninstall -y impyla
!pip install impyla

# here is the SQL you have been waiting for
from impala.dbapi import connect
conn = connect(host='10.142.0.2', port=21050)
#   172.28.210.3
cursor = conn.cursor()
cursor.execute('SELECT * FROM sample_07p LIMIT 5') 

print (cursor.description)  # prints the result set's schema
results = cursor.fetchall()
print (results)
cursor.execute('select salary, total_emp from sample_07p where total_emp < 400000 limit 500')
foo=300000*-0.0144608312694
import matplotlib.pyplot as plt
def plotit():
 for row in cursor:
    #print row[0] , row[1]
    plt.scatter(row[1],row[0])
 plt.plot([0, 300000], [50023, foo])
 plt.show()
plotit()
# leave next line to generate an error, illustrate typing at commnad prompt
print "done"

#1  export PYTHON_HOME=/opt/cloudera/parcels/Anaconda/
 #   2  export PATH=/opt/cloudera/parcels/Anaconda/bin/:$PATH
  #  3  conda install -c conda-forge impyla
   # 4  export PATH=/opt/cloudera/parcels/Anaconda/bin/:$PATH
 #   5  export PYTHON_HOME=/opt/cloudera/parcels/Anaconda/
 #   6  conda install   thrift==0.9.3
 
 
 # and now pandas and spark dataframes....
 cursor.execute('select * from sample_07p limit 10') 
from impala.util import as_pandas
df = as_pandas(cursor)
print df

type(df)

# convert to spark dataframe
 
 import os, sys
#import path
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
    .appName("Sample_07_kmeans") \
    .getOrCreate()



sc = myspark.sparkContext



from pyspark.sql import SQLContext
print sc
#df = pd.read_csv("test.csv")
print type(df)
print df
sqlCtx = SQLContext(sc)
sqlCtx.createDataFrame(df).show()


