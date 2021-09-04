from __future__ import print_function
import os
os.environ['SPARK_HOME'] = '~/spark'
import sys

from pyspark.sql import SparkSession
import time
import numpy as np
from datetime import datetime


### yellow_tripdata_1m.csv format:
"""
0 <- trip_id
1 <- start_date
2 <- end_date
3 <- start_lon
4 <- start_lat
5 <- end_lon
6 <- end_lat
7 <- fare_amount
"""
### yellow_tripvendors_1m.csv format:
"""
0 <- trip_id
1 <- vendor_id
"""

def duration(start_date, end_date):
	"""
	Finds duration of taxi trips

	Params:
	date (list of 2 strings): format:
					start date -> date[0],
					end date   -> date[1]

	Returns:
	dur (string): calculated duration from the two dates
	"""
	format = '%Y-%m-%d %H:%M:%S'
	tdelta = datetime.strptime(end_date, format) - datetime.strptime(start_date, format)
	dur = tdelta.total_seconds() / 60
	return dur

# get haversine distance
def haversine(lon_start, lat_start, lon_end, lat_end):
	"""
	Finds haversine distance of taxi trips

	Params:
	data (list of 4 strings): format:
					start lon -> lon_start,
					start lat -> lat_start,
					end lon   -> lon_end,
					end lat   -> lat_end

	Returns:
	distance (float): calculated distance from the lon,lat
	"""
	# Convert decimal degrees to radians
	lon1 = np.deg2rad(lon_start)
	lat1 = np.deg2rad(lat_start)
	lon2 = np.deg2rad(lon_end)
	lat2 = np.deg2rad(lat_end)
	# Haversine formula
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
	c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
	r = 6371 # Radius of earth in kilometers
	distance = np.around(c * r, decimals=2)
	return distance


if __name__ == '__main__':
	# Create Spark Context
	spark = SparkSession.builder.appName('query2_rdd').getOrCreate()
	sc = spark.sparkContext

	# Load data
	trip_data = sc.textFile('/data/yellow_tripdata_1m.csv')
	trip_vendors = sc.textFile('/data/yellow_tripvendors_1m.csv')


	# Split data
	trip_data = trip_data.map(lambda x: ( int(x.split(',')[0]), (x.split(',')[1], x.split(',')[2], float(x.split(',')[3]), float(x.split(',')[4]), float(x.split(',')[5]), float(x.split(',')[6]))))

	# Filter dirty writes (longitude,latitude)
	trip_data = trip_data.filter(lambda x: -80 <= x[1][2] <= -60 and -80 <= x[1][4] <= -60 and 30 <= x[1][3] <= 50 and 30 <= x[1][5] <= 50)

	# Calcuate requested metrics here for less data travelling over the network
	# lat, lon => haversine
	# dates    => duration
	trip_data = trip_data.map(lambda x: (x[0], (duration(x[1][0], x[1][1]), haversine(x[1][2], x[1][3], x[1][4], x[1][5]))))

	# Map vendor data
	vendors = trip_vendors.map(lambda x: (int(x.split(',')[0]), (int(x.split(',')[1]))))

	# Join trips and vendors
	# After join -> (trip_id, (vendor_id, (duration, haversine)))
	# Map to     -> (vendor_id, (trip_id, duration, haversine))
	joined_data = vendors.join(trip_data).\
				map(lambda x: (x[1][0], (x[0], x[1][1][0], x[1][1][1])))

	# Find max distance by vendor -> output format: (vendor_id, (trip_id, duration, max(haversine)))
	joined_data = joined_data.reduceByKey(lambda x, y: (x if x[2] >= y[2] else y)).\
					collect()

	print("\n")
	print("Vendor\tDuration\tDistance\tTripID")
	for res in joined_data:
	        print(res[0],"\t", res[1][1], "\t", res[1][2],  "\t", res[1][0])
	print("\n")

	# Stop the session
	spark.stop()
