from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf
from elasticsearch import Elasticsearch
import json

def makePair(item):
	item = item['_source']
	date = item['date']
	location = item['location']
	wind = item['wind']
	time = item['time']
	return ((location[0], location[1], date), ([time],[wind])) 

def sendOff(item):
	if len(item['date'])>5:
		es = Elasticsearch(['52.33.3.123'], http_auth=('elastic', 'changeme'), verify_certs=False)
		es.index(index='old_data1', doc_type='inputs', body=item)

def formatL(item):
	# print '\n LINE', line, '\n'
	ll = item[0][0]
	day = item[0][1]
	times = item[1][0]
	winds = item[1][1]
	doc = {"location" : ll, "date" : day, "maxWS" : max(winds), "times" : times, "winds" : winds }
	return doc

sc = SparkContext()
es = Elasticsearch(['52.33.3.123'], http_auth=('elastic', 'changeme'), verify_certs=False)
# get all docs 
test = es.search(index='stream_test')
num_docs = test['hits']['total']
res = es.search(index="stream_test", size=num_docs)#, body={"query": {"match_all": {}}})
sc.parallelize(res['hits']['hits']).map(makePair)\
.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))\
.map(formatL).foreach(sendOff)

# print res
# curl -XGET 'localhost:9200/_search?pretty'
