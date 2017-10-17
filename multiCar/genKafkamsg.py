#!/usr/bin/python
# (c) copyright Martin Lurie 2016
# sample code - not supported
import sys 
import time
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError
producer = KafkaProducer(bootstrap_servers=['gromit.lurie.biz:9092'])
for line in sys.stdin:
    	line = line.strip()
	line =  str(os.getpid()) +","+line
	producer.send('IOTtelematics', bytes(line) )
	#print line
	# generate 100 events per second
	#time.sleep(.01)
	# generate 10 events per second
	#time.sleep(.5)
	time.sleep(1)
	# 
	#time.sleep(.001)
