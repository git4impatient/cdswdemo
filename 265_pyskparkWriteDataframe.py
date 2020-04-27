# # WriteDF2Parquet
#
# This is the simplest PySpark example.create a dataframe and write it

from __future__ import print_function
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

# if java is not in the default location you'll need to specify
# spark.executorEnv.JAVA_HOME=/usr/java/yadayada
#
# secret sauce to get rid of java.lang.IllegalStateException: 
# Authentication with IDBroker
# failed. Please ensure you have a Kerberos token by using kinit.
#
# use this:  .config("spark.yarn.access.hadoopFileSystems","s3a://cdp-sandbox-default-se/datalake/warehouse")\
#

spark = SparkSession\
    .builder\
    .config('job.local.dir', 'file:///home/cdsw/')\
    .appName("WriteDF2Parquet")\
    .config("spark.authenticate", "true") \
    .config("spark.yarn.access.hadoopFileSystems","s3a://cdp-sandbox-default-se/datalake/warehouse")\
    .getOrCreate()
    
#    Create a file named spark-defaults.conf in the project or update the existing file with property:
#spark.yarn.access.hadoopFileSystems=s3a://<STORAGE LOCATION OF ENV>

# spark.yarn.access.hadoopFileSystems='s3a://cdp-sandbox-default-se/datalake/warehouse/tablespace/managed/hive/martydropme'
# this is where a create table landed
#(Note: This is the same S3 location as defined under Data Access) 


    
!klist
foo=spark.sql("show tables")
foo.take(50)
# ranger is cool with the next statement, SDX not so much...
# need the right mapping
bar=spark.sql("show create table sampletxt")
bar.take(50)

bar2=spark.sql("select * from sampletxt")
bar2.take(50)

# in ranger added marty with full access to default database
spark.sql("CREATE TABLE IF NOT EXISTS pysparktab (key INT, value STRING) USING hive")
spark.sql("insert into pysparktab values (22,'created in MLx')")
    
# DataFrames can be saved as Parquet files, maintaining the schema information.
!klist
# old debugging !hadoop fs -rm -r hdfs://se-sandbox-dl-12apr-master0.se-sandb.a465-9q4k.cloudera.site:8020/tmp/age.parquet
    
# old debugging !rm -rf age.parquet

# try to write without usinig hive 
df = spark.createDataFrame([("10", ), ("11", ), ("13",  )], ["age"])
df.show()

df.write.parquet("s3a://cdp-sandbox-default-se/datalake/martyparquet")
# this is where a create table landed hdfs://se-sandbox-dl-12apr-master0.se-sandb.a465-9q4k.cloudera.site:8020/tmp/age.parquet")
                 # file:/home/cdsw/age.parquet")
   
!hadoop fs -ls s3a://cdp-sandbox-default-se/datalake/
   
spark.stop()
