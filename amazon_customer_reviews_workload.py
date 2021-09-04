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
        spark = SparkSession.builder.appName('amazon_customer_reviews_workload').getOrCreate() #.config("spark.sql.shuffle.partitions", "1000").config("spark.default.parallelism", "1000")
        sc = spark.sparkContext

        pc_data = sc.textFile('/data/amazon_reviews_us_PC_v1_00.tsv').filter(lambda l: not l.startswith("marketplace"))

        # Split data, keep unique lines based on unique review_id
        # Final format: {review_id -> (customer_id, product_id, product_parent, product_title, product_category, star_rating, review_headline, review_body)}
        pc_data = pc_data.map(lambda x: ( x.split('\t')[2] , (x.split('\t')[0], x.split('\t')[1], x.split('\t')[3], x.split('\t')[4], x.split('\t')[5], x.split('\t')[6], int(x.split('\t')[7]), x.split('\t')[8], x.split('\t')[9], x.split('\t')[10], x.split('\t')[11], x.split('\t')[12], x.split('\t')[13], x.split('\t')[14]))).\
                reduceByKey(lambda x,y: (x))

        # Self Join PC data based on review_id
        # After join: {customer_id -> list_of(review_id, product_id, star_rating, review_headline) with size of 2}
        joined_data = pc_data.join(pc_data).\
                map(lambda x: (1)).\
                count()
                #map(lambda x: (x[0], len(x[1]))).\
                #map(lambda x: (1)).\
                #collect()
        print("\n")
        #for res in joined_data:
        #        print(res[0],"\t", res[1])
        print("Result:", joined_data)
        print("\n")

        # Stop the session
        spark.stop()


