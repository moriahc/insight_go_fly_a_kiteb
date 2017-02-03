from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
from elasticsearch import Elasticsearch
# import station_dict
import sys, json
# import statistics

wban = None
date = None
time = None
wind = None
stations = None
# es = Elasticsearch(['52.33.3.123'], http_auth=('elastic', 'changeme'), verify_certs=False)


def isHeader(line):
	return line.split(',')[0] in ["WBAN", "wban", "Wban", " WBAN", " wban", 'Wban Number']


def getHeader(header):
	global wban
	global date
	global time
	global wind
	if len(header.split(", ")) > 5:
		header = header.split(", ")
	elif len(header.split(",")) > 5:
		header = header.split(",")
	else:
		raise ValueError("Current splitting not working: "+ header)
	if 'WBAN' in header:
		wban = header.index('WBAN')
	elif 'Wban Number' in header:
		wban = header.index('Wban Number')
	elif 'wban' in header:
		wban.index('wban')
	else:
		raise ValueError('WBAN not recognized')
	if 'Date' in header:
		date = header.index('Date')
	elif 'YearMonthDay' in header:
		date = header.index("YearMonthDay")
	else:
		raise ValueError('date not recognized')
	time = header.index('Time')
	if 'WindSpeed' in header:
		wind = header.index('WindSpeed') #24
	elif 'Wind Speed (kt)' in header:
		wind = header.index('Wind Speed (kt)')
	else:
		raise ValueError('windspeed not recognized')
	return wban, date, time, wind

# FUNCT FOR .MAP
def getUseful(line):
	if not isHeader(line):
		data = line.split(",")
		wban_i = data[wban]
		date_i = data[date]
		time_i = data[time]
		wind_i = data[wind]
		try:
			wind_i = int(wind_i)
		except:
			wind_i = None
		if wban_i in stations.keys():
			return ((stations[wban_i][0], stations[wban_i][1], date_i), ([time_i], [wind_i]))
		else:
			return ('wban not in existance', 'trash')
	else:
		return ("header", "stuff")


def formatL(line):
	lat = line[0][0]
	lon = line[0][1]
	day = line[0][2]
	times = line[1][0]
	winds = line[1][1]
	doc = {"location" : [lon, lat], "date" : day, "maxWS" : max(winds), "times" : times, "winds" : winds }
	return doc

def sendOff(line):
	# from elasticsearch import Elasticsearch
	if len(line['date'])>5:
		es = Elasticsearch(['52.33.3.123'], http_auth=('elastic', 'changeme'), verify_certs=False)
		es.index(index='s3_historical_data', doc_type='inputs', body=line)


def main():
	conn = S3Connection('<aws access key>','<aws secret access key>')
	bucket = conn.get_bucket('moriah-historical-noaa-data')
	sc = SparkContext('spark://ip-172-31-1-197.us-west-2.compute.internal:7077')
	global stations
	with open('pysrc/all_stations.json') as data_file:
		stations = json.load(data_file)
	# stations = station_dict.getDict("pysrc/201701station.txt")
	# path = filename
	path = "s3n://moriah-historical-noaa-data/"
	for fyle in bucket:
		print "\n FILE NAME: ", fyle.name, "\n"
		rdd = sc.textFile(path+fyle.name)
		# rdd = sc.textFile(path+"test_hourly.txt")
		# print "first line rdd: ", rdd.take(1)
		getHeader(rdd.take(1)[0])
		l1 = rdd.map(getUseful).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
		# print "\n POST REDUCE, ", ex ,"\n" , "len: ", len(ex), '\n'
		# print '\n 32nd element: ', ex[32], ' lat ', ex[0][0][0]
		l2 = l1.map(formatL)
		# print '\n formatted', l2.collect(), '\n'
		l2.foreach(sendOff)

if __name__ == '__main__':
	# input_txt = sys.argv[1]
	# print "\n", "input_txt: ", input_txt, '\n'
	# main(input_txt)
	main()
