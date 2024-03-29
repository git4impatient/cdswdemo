from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer

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
# use this:  .config("spark.yarn.access.hadoopFileSystems","s3a://box-default-se/datalake/warehouse")\
#

spark = SparkSession\
    .builder\
    .config('job.local.dir', 'file:///home/cdsw/')\
    .appName("WriteDF2Parquet")\
    .config("spark.authenticate", "true") \
    .config("spark.yarn.access.hadoopFileSystems",\
    "s3a://locationOfYourS3bucket/sandbox/warehouse/")\
    .getOrCreate()
    
# Prepare training documents from a list of (id, text, label) tuples.
training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

# Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)

# Prepare test documents, which are unlabeled (id, text) tuples.
test = spark.createDataFrame([
    (4, "spark i j k"),
    (5, "l m n"),
    (6, "spark hadoop spark"),
    (7, "apache hadoop")
], ["id", "text"])

# Make predictions on test documents and print columns of interest.
prediction = model.transform(test)
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row  # type: ignore
    print(
        "(%d, %s) --> prob=%s, prediction=%f" % (
            rid, text, str(prob), prediction   # type: ignore
        )
    )
