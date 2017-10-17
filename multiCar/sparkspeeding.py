# copyright 2017 Martin Lurie
import sys
import pprint

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
sc = SparkContext(appName="PythonSpeedingAlerts")
ssc = StreamingContext(sc, 5)
ssc.checkpoint("hdfs://gromit:8020/user/marty/IOTtelematicscheckpoints")   # set checkpoint directory
print ssc

kvs = KafkaUtils.createDirectStream(ssc, ['IOTtelematics'], {"metadata.broker.list": 'gromit:9092'})
print kvs

lines = kvs.map(lambda x: x[1])
vin = lines.map(lambda line: line.split(",")[0]) 
pairs = vin.map(lambda vin: (vin, 1)) 
vinCounts = pairs.reduceByKey(lambda a, b: a+b)
print "current vin counts:"
print "last 40 seconds of vin counts every 20 seconds"
# print 5 lines of the vinCounts from the current window
vinCounts.pprint(num=5)
# look back over 40 seconds, every 20 seconds print out the action from the last 40 seconds
# of this data, print 15 lines
windowedVinCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 40, 20)
windowedVinCounts.filter(lambda q: int(q[1])>5).pprint(num=15)
windowedVinCounts2 = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 80, 10)
windowedVinCounts2.filter(lambda q: int(q[1])>5).pprint(num=20)



ssc.start()
ssc.awaitTermination()

