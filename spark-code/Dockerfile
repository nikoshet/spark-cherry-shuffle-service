# base image
FROM openjdk:8-jdk
#FROM openjdk:8-jdk-slim

RUN mkdir spark && \
    apt-get update && \
    apt-get install -y maven scala
WORKDIR spark
ADD spark-code/code ./

RUN mvn -DskipTests clean package

# add scripts and update spark default config
ADD common.sh spark-master.sh spark-worker.sh spark-driver.sh spark-cherry-shuffle-service.sh spark-metadata-service.sh  ./
ADD conf/spark-defaults.conf ./conf/spark-defaults.conf
ADD conf/spark-env.sh ./conf/spark-env.sh
ADD conf/log4j.properties ./conf/log4j.properties
ADD conf/metrics.properties ./conf/metrics.properties
ADD spark-examples_2.12-3.1.0-SNAPSHOT.jar ./

# python testing
RUN apt-get update && apt-get install -y python3 python3-setuptools python3-pip && pip3 install pyspark numpy
RUN PATH="$HOME/bin:$HOME/.local/bin:$PATH:~/spark/bin" && SPARK_HOME=~/spark  && export SPARK_HOME=~/spark && export PYSPARK_PYTHON=python3 && python3 --version
# + add PYSPARK_PYTHON="/usr/bin/python3.7" in conf/spark-env.sh
ADD query2_rdd.py ./
ADD amazon_customer_reviews_workload.py ./
ADD amazon_customer_reviews_workload_v1.py ./
ADD amazon_customer_reviews_workload_v2.py ./
ADD synthetic_workload.py ./
ADD skewed_synthetic_workload.py ./

RUN chmod +x *.sh