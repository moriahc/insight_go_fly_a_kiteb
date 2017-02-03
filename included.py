import os, sys, json, urllib2, random
import station_dict_str

# textfile1 = '200705station.txt'
# textfile2 = '200706station.txt'

# d1 = station_dict_str.getDict(textfile1)
# d2 = station_dict_str.getDict(textfile2)
# a = True
# print d1, '\n', len(d1.keys()), ' ', len(d2.keys())
# for k in d1.keys():
# 	if k not in d2.keys():
# 		print "wban not in d2: ", k
# 		a = False
# print a

master_dict = {}
folder = []
for year in ['2007', '2008', '2009','2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']:
	for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
		if year >'2007' and year<'2017':
			folder.append(year+month+'station.txt')
		elif year is '2007' and month > '05':
			folder.append(year+month+'station.txt')
		elif year is '2017' and month <'02':
			folder.append(year+month+'station.txt')


for fyle in folder:
	text = open(fyle, 'r')
	d = station_dict_str.getDict(fyle)
	master_dict = dict(master_dict.items()+d.items())

print 'keys num: ', len(master_dict.keys())
with open('all_stations.json', 'w') as fp:
    json.dump(master_dict, fp)