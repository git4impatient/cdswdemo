import cml.data_v1 as cmldata

# create the database table default.stage before running this code

# Sample in-code customization of spark configurations
#from pyspark import SparkContext
#SparkContext.setSystemProperty('spark.executor.cores', '1')
#SparkContext.setSystemProperty('spark.executor.memory', '2g')

CONNECTION_NAME = "se-aws-edl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Sample usage to run query through spark
EXAMPLE_SQL_QUERY = "show databases"
spark.sql(EXAMPLE_SQL_QUERY).show()


#from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
#spark = SparkSession.builder \
#    .appName("InsertIntoHiveTable") \
#    .enableHiveSupport() \
#    .getOrCreate()

# Define file path
file_path = "mydata.txt"

# Define schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("myrowid", IntegerType(), True),
    StructField("mymonth", IntegerType(), True),
    StructField("myweekday", IntegerType(), True),
    StructField("mydayofyear", IntegerType(), True),
    StructField("myonein10k", IntegerType(), True),
    StructField("mystring", StringType(), True)
])

# Read the file into a DataFrame
df = spark.read \
    .option("delimiter", "|") \
    .schema(schema) \
    .csv(file_path)

# Show the DataFrame (optional, for debugging)
df.show()

# Insert data into existing Hive table
df.write \
    .mode("append") \
    .insertInto("default.stage")

# Stop the Spark session
spark.stop()
