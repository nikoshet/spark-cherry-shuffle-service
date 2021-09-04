from __future__ import print_function
import os
os.environ['SPARK_HOME'] = '~/spark'
import sys

from pyspark.sql import SparkSession
import time
import numpy as np
from datetime import datetime


### format:
  # DATA COLUMNS:
  # 0:  marketplace       - 2 letter country code of the marketplace where the review was written.
  # 1:  customer_id       - Random identifier that can be used to aggregate reviews written by a single author.
  # 2:  review_id         - The unique ID of the review.
  # 3:  product_id        - The unique Product ID the review pertains to. In the multilingual dataset the reviews
  #                         for the same product in different countries can be grouped by the same product_id.
  # 4:  product_parent    - Random identifier that can be used to aggregate reviews for the same product.
  # 5:  product_title     - Title of the product.
  # 6:  product_category  - Broad product category that can be used to group reviews
  #                         (also used to group the dataset into coherent parts).
  # 7:  star_rating       - The 1-5 star rating of the review.
  # 8:  helpful_votes     - Number of helpful votes.
  # 9:  total_votes       - Number of total votes the review received.
  # 10: vine              - Review was written as part of the Vine program.
  # 11: verified_purchase - The review is on a verified purchase.
  # 12: review_headline   - The title of the review.
  # 13: review_body       - The review text.
  # 14: review_date       - The date the review was written.


if __name__ == '__main__':
	# Create Spark Context
	spark = SparkSession.builder.appName('amazon_customer_reviews_workload').getOrCreate()
	sc = spark.sparkContext

	# Load data
	software_data = sc.textFile('/data/amazon_reviews_us_Software_v1_00.tsv').filter(lambda l: not l.startswith("marketplace"))
	video_games_data = sc.textFile('/data/amazon_reviews_us_Video_Games_v1_00.tsv').filter(lambda l: not l.startswith("marketplace"))
	pc_data = sc.textFile('/data/amazon_reviews_us_PC_v1_00.tsv').filter(lambda l: not l.startswith("marketplace"))


	# Split data, keep unique lines based on unique review_id and filter reviews with star_rating>2
	# Final format: {customer_id -> (review_id, product_id, star_rating, review_body)}
	software_data = software_data.map(lambda x: ( x.split('\t')[2] , (x.split('\t')[1], x.split('\t')[3], int(x.split('\t')[7]), x.split('\t')[13]))).\
		reduceByKey(lambda x,y: (x)).\
		map(lambda x: ( x[1][0] , (x[0], x[1][1], x[1][2], x[1][3]))).\
		filter(lambda x: x[1][2] > 3)

	video_games_data = video_games_data.map(lambda x: ( x.split('\t')[2] , (x.split('\t')[1], x.split('\t')[3], int(x.split('\t')[7]), x.split('\t')[13]))).\
		reduceByKey(lambda x,y: (x)).\
		map(lambda x: ( x[1][0] , (x[0], x[1][1], x[1][2], x[1][3]))).\
		filter(lambda x: x[1][2] > 3)

	pc_data = pc_data.map(lambda x: ( x.split('\t')[2] , (x.split('\t')[1], x.split('\t')[3], int(x.split('\t')[7]), x.split('\t')[13]))).\
		reduceByKey(lambda x,y: (x)).\
		map(lambda x: ( x[1][0] , (x[0], x[1][1], x[1][2], x[1][3]))).\
		filter(lambda x: x[1][2] > 3) #filter reviews with star_rating>1


	# 1st Join Software and Video Games data based on customer_id
	# After join: {customer_id -> list_of(review_id, product_id, star_rating, review_body)}
	# After filter: keep only rows that have 2+ reviews from each customer
	# After flatMapValues: {customer_id -> (review_id, product_id, star_rating, review_body)}
	joined_data = software_data.join(video_games_data).\
		filter(lambda x: len(x[1]) >= 2).\
		flatMapValues(lambda x: x).\
		cache()
		# filter(lambda x: x[1][2]>4).cache()

	# 1st Join Joined Data and PC data based on customer_id
	# After join: {customer_id -> list_of(review_id, product_id, star_rating, review_body)}
	# After flatMapValues: {customer_id -> (review_id, product_id, star_rating, review_body)}
	# After map: {review_body}
	# After: Simple WordCount in upper letters
	new_joined_data = joined_data.join(pc_data).\
		flatMapValues(lambda x: x).\
		map(lambda x: (x[1][3].upper())).\
		flatMap(lambda x: x.split(" ")).\
		map(lambda x: (x, 1)).\
		reduceByKey(lambda x, y: x + y).\
		sortBy(lambda x: x[1]).\
		collect()
		#take(5)

	print("\n")
	for res in new_joined_data:
		print(res[0],"\t", res[1])
	print("\n")

	# Stop the session
	spark.stop()
