from __future__ import print_function
import os
os.environ['SPARK_HOME'] = '~/spark'
import sys

from pyspark.sql import SparkSession
import time
import numpy as np
from datetime import datetime
import random
import string


if __name__ == '__main__':
        # Create Spark Context
        spark = SparkSession.builder.appName('synthetic_workload').getOrCreate()
        sc = spark.sparkContext

        partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 100
        size = float(sys.argv[2]) if len(sys.argv) > 0 else 1

        #n = 10000000 # 1 GB dataset
        #n = 100000000 # 10 GB dataset
        n = int(10000000 * size)

        N=100
        data = sc.parallelize(range(1 + partitions, n + 1 + partitions), partitions).\
            map(lambda x: (x % partitions, (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N)))))

        #res = data.join(data).\ -> result:row_of_lines^2
        #    count() # map(lambda x: (1)).\
        #res = data.count()
        res = data.groupByKey().\
            mapValues(lambda x: len(x)).\
            collect()

        #print("\n")
        print("Result:", res)
        #for res in data:
        #    print(res, "\n")
            #print(res[0],"\t", res[1])
        print("\n")


        # Stop the session
        spark.stop()
