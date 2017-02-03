import os, sys, json, urllib2, random
import pylab as pl
from kafka import KafkaConsumer, KafkaProducer
# import station_dict_str
import datetime

# brooker
producer = KafkaProducer(bootstrap_servers='localhost:9092')
mytopic = "test_topic"
# stations = None

"""call the noaa api for the lat lon of each station"""
def get_stations(stations):
	# station_d = station_dict_str.getDict(filename)	
	for key in stations:
		current = call_wind_api(stations[key][0], stations[key][1])


def simulate(lat, lon, wind,date, time):
	global mytopic
	max_lat = lat + 1.0000
	min_lat = lat - 1.0000
	max_lon = lon + 1.0000
	min_lon = lon - 1.0000
	# change when higher resolution wanted
	lat_interval = 0.05
	lon_interval = 0.05
	lat_range = pl.frange(min_lat, max_lat, lat_interval)
	lon_range = pl.frange(min_lon, max_lon, lon_interval)
	# if lat in lat_range:
	# 	lat_range.remove(lat)
	# if lon in lon_range:
	# 	lon_range.remove(lon)
	for i in lat_range:
		for j in lon_range:
			# create random num between -1 and 1
			# wind_i = wind + wind * num
			gust = random.uniform(-1, 1)
			wind_i = wind + (wind * gust)
			if i is not lat and j is not lon:
				data = (str(str(lon)+','+str(lat)+','+str(int(wind))+','+date+','+ time))
				producer.send(mytopic, data)
				print 'wind_sim: ', wind_i 

		# send lon, lat, wind to kafka

# consumer = KafkaConsumer('my-topic',
#                          group_id='my-group',
#                          bootstrap_servers=['localhost:9092'])
# client = KafkaClient(hosts="127.0.0.1:9092")
# list_of_topics = client.topics
# test_topic = list_of_topics['test']
# with test_topic.get_sync_producer() as producer:
# 	for i in range(4):
# 		producer.produce(str(data))

# consumer = test_topic.get_simple_consumer()
# for message in consumer:
#     if message is not None:
#          print message.value #QUESTION: message.offset idk what it is




def call_wind_api(lat, lon):
	str_url = "http://forecast.weather.gov/MapClick.php?lat="
	url_json = urllib2.urlopen(str_url + str(lat) + "&lon=" + str(lon) + "&FcstType=json")
	js_url = url_json.read()
	info = json.loads(js_url)
	date_time = str(datetime.datetime.now()).replace(':', '').replace('-', '')
	date = date_time[0:8]
	time = date_time[9:15]
	wind = 0
	try:
		wind = info[u'currentobservation'][u'Winds']
		print "wind: ", wind
		# wind = curr
	except:
		print "Station down: ", lat, lon
		pass
	try:
		wind=int(wind)
	except:
		wind = 0
	data = (str(str(lon)+','+str(lat)+','+str(wind)+','+date+','+ time))
	producer.send(mytopic, data)
	simulate(float(lat), float(lon), wind, date, time)

def main():
	# global stations
	with open('all_stations.json') as data_file:
		stations = json.load(data_file)
	# stations = {'00102': [34.09972, -93.06583], '00108' : [61.85, -165.567], '00109': [58.983, -159.05]}
	for i in range(0, 1000):
		get_stations(stations)

if __name__ == '__main__':
	main()
