#!/bin/bash

if ! getent hosts spark-master; then
  sleep 5
  exit 0
fi


echo "$(hostname -i) spark-driver"
echo "$(hostname -i) spark-driver" >> /etc/hosts
#echo "$(hostname -i) spark-driver.default.svc.cluster.local" >> /etc/hosts


#hostname -s 127.0.0.1

#export NAMESPACE=default
#export DRIVER_NAME=spark-driver
#export SPARK_LOCAL_IP=127.0.0.1
# spark.driver.port?
# spark.driver.host?
# spark.kubernetes.namespace=default?
# --conf spark.local.dir=/home/ubuntu/tmp-spark \

# for i in {1..10}; do COMMAND ; done
#exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

mkdir /tmp/spark-events

####----------- FLAGS TO USE -----------####
## -s: service
# values: cherry, external, none
## -w: workload
# values: file.py, file.jar
## -c: class
# values: java.class if workload==jar
# -d: dataset
# values: .cvs, null, etc
# -p: parallelism
# values: number
# -l: look-ahead caching
# values: boolean(default:false)
# -r: look-ahead caching port
# values: number(default: 7788)

### Examples ###
## /spark/spark-driver.sh -s cherry -w amazon_customer_reviews_workload.py -p 100

## /spark/spark-driver.sh -s none -w /spark/examples/src/main/python/wordcount.py -d /file.csv -p 100

## /spark/spark-driver.sh -s external -w file://~/spark/spark-examples_2.12-3.1.0-SNAPSHOT.jar \
# -c org.apache.spark.examples.JavaPageRank -d /file.txt -p 100

## /spark/spark-driver.sh -s cherry -w tpcds -p 100 -q "q2,q5"
### /Examples ###

while getopts "s:w:c:d:p:q:g:l:r:z:k:" opt; do
  case $opt in
    s) service=$OPTARG     ;;
    w) workload=$OPTARG    ;;
    c) class=$OPTARG       ;;
    d) dataset=$OPTARG     ;;
    p) parallelism=$OPTARG ;;
    q) queries=$OPTARG ;;
    g) gigabytes=$OPTARG ;;
    l) caching=$OPTARG     ;;
    r) cachingPort=$OPTARG    ;;
    z) distributed=$OPTARG    ;;
    k) skewness=$OPTARG     ;;
    *) echo 'error' >&2
       exit 1
  esac
done


if [ $service = "cherry" ]; then
  echo "Using CHERRY Shuffle Service..."

  if [[ $caching = "true" ]]; then
    echo "Using Look-Ahead caching.."

    if [[ $distributed = "true" ]]; then
      echo "Using Distributed Cherry System.."

      if [[ $workload == *"py"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"jar"* ]];  then
        ./bin/spark-submit \
          --class $class \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"tpcds"* ]];  then
        ./bin/spark-submit \
          --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.sql.shuffle.partitions=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          --conf spark.sql.parquet.compression.codec=snappy \
          --verbose \
          --jars /spark/core/target/spark-core_2.12-3.1.0-SNAPSHOT-tests.jar,/spark/sql/catalyst/target/spark-catalyst_2.12-3.1.0-SNAPSHOT-tests.jar \
          /spark/sql/core/target/spark-sql_2.12-3.1.0-SNAPSHOT-tests.jar \
          --data-location hdfs://10.0.1.167:6666/home/ubuntu/tpcds \
          --query-filter $queries

      elif [[ $workload == *"synthetic"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          synthetic_workload.py $parallelism $gigabytes

      elif [[ $workload == *"skew"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          skewed_synthetic_workload.py $parallelism $gigabytes $skewness
      fi

    elif [ -z "$distributed" ] || [ $distributed = "false" ]; then

      if [[ $workload == *"py"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"jar"* ]];  then
        ./bin/spark-submit \
          --class $class \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"tpcds"* ]];  then
        ./bin/spark-submit \
          --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.sql.shuffle.partitions=$parallelism \
          --conf spark.sql.parquet.compression.codec=snappy \
          --verbose \
          --jars /spark/core/target/spark-core_2.12-3.1.0-SNAPSHOT-tests.jar,/spark/sql/catalyst/target/spark-catalyst_2.12-3.1.0-SNAPSHOT-tests.jar \
          /spark/sql/core/target/spark-sql_2.12-3.1.0-SNAPSHOT-tests.jar \
          --data-location hdfs://10.0.1.167:6666/home/ubuntu/tpcds \
          --query-filter $queries

      elif [[ $workload == *"synthetic"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          synthetic_workload.py $parallelism $gigabytes

      elif [[ $workload == *"skew"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.enabled=true \
          --conf spark.shuffle.service.look.ahead.caching.port=$cachingPort \
          --conf spark.default.parallelism=$parallelism \
          skewed_synthetic_workload.py $parallelism $gigabytes $skewness
      fi
    fi

  else

    if [[ $distributed = "true" ]]; then
      echo "Using Distributed Cherry System.."

      if [[ $workload == *"py"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"jar"* ]];  then
        ./bin/spark-submit \
          --class $class \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"tpcds"* ]];  then
        ./bin/spark-submit \
          --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.sql.shuffle.partitions=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          --conf spark.sql.parquet.compression.codec=snappy \
          --verbose \
          --jars /spark/core/target/spark-core_2.12-3.1.0-SNAPSHOT-tests.jar,/spark/sql/catalyst/target/spark-catalyst_2.12-3.1.0-SNAPSHOT-tests.jar \
          /spark/sql/core/target/spark-sql_2.12-3.1.0-SNAPSHOT-tests.jar \
          --data-location hdfs://10.0.1.167:6666/home/ubuntu/tpcds \
          --query-filter $queries

      elif [[ $workload == *"synthetic"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          synthetic_workload.py $parallelism $gigabytes

      elif [[ $workload == *"skew"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.distributed.cherry.enabled=true \
          --conf spark.metadata.service.host=spark-metadata-service \
          --conf spark.metadata.service.port=5555 \
          skewed_synthetic_workload.py $parallelism $gigabytes $skewness
      fi

    elif [ -z "$distributed" ] || [ $distributed = "false" ]; then

      if [[ $workload == *"py"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"jar"* ]];  then
        ./bin/spark-submit \
          --class $class \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          $workload \
          $dataset \
          --verbose

      elif [[ $workload == *"tpcds"* ]];  then
        ./bin/spark-submit \
          --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          --conf spark.sql.shuffle.partitions=$parallelism \
          --conf spark.sql.parquet.compression.codec=snappy \
          --verbose \
          --jars /spark/core/target/spark-core_2.12-3.1.0-SNAPSHOT-tests.jar,/spark/sql/catalyst/target/spark-catalyst_2.12-3.1.0-SNAPSHOT-tests.jar \
          /spark/sql/core/target/spark-sql_2.12-3.1.0-SNAPSHOT-tests.jar \
          --data-location hdfs://10.0.1.167:6666/home/ubuntu/tpcds \
          --query-filter $queries

      elif [[ $workload == *"synthetic"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          synthetic_workload.py $parallelism $gigabytes

      elif [[ $workload == *"skew"* ]];  then
        ./bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=spark-driver \
          --conf spark.driver.cores=1 \
          --conf spark.driver.memory=1g \
          --conf spark.eventLog.enabled=true \
          --conf spark.shuffle.service.availability="cherry" \
          --conf spark.shuffle.service.enabled=true \
          --conf spark.shuffle.service.port=7777 \
          --conf spark.shuffle.service.host=spark-cherry-shuffle-service \
          --conf spark.ui.prometheus.enabled=true \
          --conf spark.default.parallelism=$parallelism \
          skewed_synthetic_workload.py $parallelism $gigabytes $skewness
      fi
    fi
  fi


elif [ $service = "external" ]; then
  echo "Using External Shuffle Service..."

  if [[ $workload == *"py"* ]];  then
    ./bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.availability="local" \
      --conf spark.shuffle.service.enabled=true \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      $workload \
      $dataset \
      --verbose

  elif [[ $workload == *"jar"* ]];  then
    ./bin/spark-submit \
      --class $class \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.availability="local" \
      --conf spark.shuffle.service.enabled=true \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      $workload \
      $dataset \
      --verbose

  elif [[ $workload == *"tpcds"* ]];  then
    ./bin/spark-submit \
      --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.availability="local" \
      --conf spark.shuffle.service.enabled=true \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      --conf spark.sql.shuffle.partitions=$parallelism \
      --conf spark.sql.parquet.compression.codec=snappy \
      --verbose \
      --jars /spark/core/target/spark-core_2.12-3.1.0-SNAPSHOT-tests.jar,/spark/sql/catalyst/target/spark-catalyst_2.12-3.1.0-SNAPSHOT-tests.jar \
      /spark/sql/core/target/spark-sql_2.12-3.1.0-SNAPSHOT-tests.jar \
      --data-location hdfs://10.0.1.167:6666/home/ubuntu/tpcds \
      --query-filter $queries

  elif [[ $workload == *"synthetic"* ]];  then
    ./bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.availability="local" \
      --conf spark.shuffle.service.enabled=true \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      synthetic_workload.py $parallelism $gigabytes

  elif [[ $workload == *"skew"* ]];  then
    ./bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.availability="local" \
      --conf spark.shuffle.service.enabled=true \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      skewed_synthetic_workload.py $parallelism $gigabytes $skewness
  fi


elif [ $service = "none" ]; then
  echo "Using no Shuffle Service..."

  if [[ $workload == *"py"* ]];  then
    ./bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.enabled=false \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      $workload \
      $dataset \
      --verbose

  elif [[ $workload == *"jar"* ]];  then
    ./bin/spark-submit \
      --class $class \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.enabled=false \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      $workload \
      $dataset \
      --verbose

  elif [[ $workload == *"tpcds"* ]];  then
    ./bin/spark-submit \
      --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.enabled=false \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      --conf spark.sql.shuffle.partitions=$parallelism \
      --conf spark.sql.parquet.compression.codec=snappy \
      --verbose \
      --jars /spark/core/target/spark-core_2.12-3.1.0-SNAPSHOT-tests.jar,/spark/sql/catalyst/target/spark-catalyst_2.12-3.1.0-SNAPSHOT-tests.jar \
      /spark/sql/core/target/spark-sql_2.12-3.1.0-SNAPSHOT-tests.jar \
      --data-location hdfs://10.0.1.167:6666/home/ubuntu/tpcds \
      --query-filter $queries

  elif [[ $workload == *"synthetic"* ]];  then
    ./bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.enabled=false \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      synthetic_workload.py $parallelism $gigabytes

  elif [[ $workload == *"skew"* ]];  then
    ./bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --conf spark.driver.host=spark-driver \
      --conf spark.driver.cores=1 \
      --conf spark.driver.memory=1g \
      --conf spark.eventLog.enabled=true \
      --conf spark.shuffle.service.enabled=false \
      --conf spark.ui.prometheus.enabled=true \
      --conf spark.default.parallelism=$parallelism \
      skewed_synthetic_workload.py $parallelism $gigabytes $skewness
  fi

fi