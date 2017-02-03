import os, sys, json, urllib2, random


def getDict(filename):
	lines = open(filename, 'r').readlines()
	header = lines[0].split("|")
	s_dict = {}
	# print "HEADER: ", header
	if 'WBAN' in header:
		wban = header.index('WBAN')
	elif 'Wban Number' in header:
		wban = header.index('Wban Number')
	elif 'wban' in header:
		wban = wban.index('wban')
	else:
		raise ValueError('WBAN not recognized')
	if 'Longitude' in header:
		lon = header.index('Longitude')
	elif 'longitude' in header:
		lon = header.index("longitude")
	else:
		raise ValueError('longitude not recognized')
	if 'Latitude' in header:
		lat = header.index('Latitude') #24
	elif 'latitude' in header:
		lat = header.index('latitude')
	else:
		raise ValueError('latitiude not recognized')
	# print "WBAN: ", wban, " LAT: ", lat, " LON: ", lon
	# file_all = lines.collect()
	file_all = lines
	for i in range(1, len(file_all)):
		data = file_all[i].split('|')
		# print "DATA: ", data	
		wban_i = data[wban]
		lon_i = data[lon]
		lat_i = data[lat]
		s_dict[wban_i] = (float(lat_i), float(lon_i))
	return s_dict
