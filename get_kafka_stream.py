from __future__ import print_function 
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
import statistics
import redis


# r.set('foo', 'bar')
def getFormat(line):
	lon, lat, wind, date, time = line[1].split(',')
	try:
		wind = int(wind)
	except:
		wind = None
	return {'location':[lon, lat], 'date' : date, 'time' : time, 'wind' : wind}
	# return line

def sendOff(line):
	es = Elasticsearch(['52.33.3.123'], http_auth=('elastic', 'changeme'), verify_certs=False)
	es.index(index='stream_test', doc_type='inputs', body=line)

def getLLW(line):
	lon, lat, wind, _, _ = line[1].split(',')
	try:
		wind = int(wind)
	except:
		wind = None
	return ((lon, lat), ([wind], wind, 1))


def getStoke(st_dev, wind):
	a = (( -.25 * wind) ** 2 + (15 * wind) - 125)
	if a >0:
		return a / (1 + st_dev)
	else:
		return 0

def getNums(item):
	try:
		wind = item[1]
		key = item[0]
	except:
		wind = []
	if len(wind)>2:
		st_dev = statistics.stdev(wind)
	# if None in wind:
		stoke = getStoke(st_dev, wind)
		return (key, stoke) 
	else: 
		return ('loser', 0)

def send2redis(item):
	# define function to put kv pair in redis
	redis_server = 'ec2-52-35-127-177.us-west-2.compute.amazonaws.com'
	r = redis.StrictRedis(host=redis_server, port=6379, db=0)
	r.set([item[0][0], item[0][1]], item[1])


def numerator(item):
	key, (arr, sum_a, count) = item
	for i in range(0, len(arr)):
		arr[i] = (arr[i]-(sum_a/count))**2
	return (key, (arr, count))

def main():
	mytopic = "test_topic"
	sc = SparkContext('spark://ip-172-31-1-196.us-west-2.compute.internal:7077')
	ssc = StreamingContext(sc,1) #optional batch duration
	ssc.checkpoint("checkpoint")
	brokers = 'ec2-35-163-168-63.us-west-2.compute.amazonaws.com:9092'
	# directKafkaStream1 = KafkaUtils.createDirectStream(ssc, [mytopic], {"metadata.broker.list": brokers})
	directKafkaStream2 = KafkaUtils.createDirectStream(ssc, [mytopic], {"metadata.broker.list": brokers})
	win = directKafkaStream2.window(600, 300)
	winmr = win.map(getLLW).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2])).map(lambda a: (a[0], getStoke(statistics.stdev(a[1][0]), statistics.mean(a[1][0]))))
	winmr.foreachRDD(lambda rdd: rdd.foreach(send2redis))
	# lambda a: a[0], [(x-(a[1][1]/a[1][2]))**2 for x in a[1][0]]
	# winmrmm = winmr.map(numerator).map(lambda a: (a[0], (math.sqrt(sum(a[1][0])/(a[1][2]-1)), a[1][1])))
	winmr.pprint()
	# ws = directKafkaStream2.map(lambda a : a[1].split(',')[2])
	# ws.reduceByKeyAndWindow(lambda a, b: a+b, lambda a, b: a-b, 30, 3).pprint()

	# window = directKafkaStream2.map(getLLW)
	# window1 = window.reduceByKeyAndWindow(lambda a, b : a+b, lambda a, b: a-b, 60, 10)\
	# # .foreachRDD(lambda rdd :rdd.pprint())	# window1.foreachRDD(printrdd)
	# # window1.foreachRDD(printrdd)
	# # window2 = window1.foreachRDD(printrdd)
	# # print('\n This:'+str(window.take(1)))
	# # window1.map(getNums)\
	# window1.pprint()
	stream = directKafkaStream2.map(getFormat)
	print("\n \n")
	stream.pprint()
	print("\n \n")
	stream.foreachRDD(lambda rdd : rdd.foreach(sendOff))

	ssc.start()
	ssc.awaitTermination()

if __name__ == '__main__':
	main()


