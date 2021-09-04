/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCountTestWith2MapReduceStages {
  private static final Pattern SPACE = Pattern.compile(" ");
  private static final Pattern COMMA = Pattern.compile(",");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWordCount")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    //JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(COMMA.split(s)).iterator())
            .flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> filtered_ones = ones.filter(s -> (s._1.length()>0));

    JavaPairRDD<String, Integer> counts = filtered_ones.reduceByKey((i1, i2) -> i1 + i2);

    //JavaPairRDD<String, Integer> added = counts.mapValues(i -> i+10);
    //JavaPairRDD<String, Integer> ordered = added.sortByKey(false);
    //List<Tuple2<String, Integer>> output = ordered.collect();

    JavaPairRDD<String, Integer> new_rdd = counts.mapToPair(s ->
    {
      /*if (s._1.length() <=4)
        return new Tuple2<String, Integer>(s._1, 1);
      else
        return new Tuple2<String, Integer>(s._1.substring(0,4), 1);*/
      return new Tuple2<String, Integer>(s._1.substring(0,1), s._2);
    });
    JavaPairRDD<String, Integer> new_counts = new_rdd.reduceByKey((i1, i2) -> i1 + i2);
    JavaPairRDD<Integer, String> changed_rdd = new_counts.mapToPair(s -> new Tuple2<Integer, String>(s._2, s._1));
    JavaPairRDD<Integer, String> ordered = changed_rdd.sortByKey(false);
    List<Tuple2<Integer, String>> output = ordered.collect();

    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    spark.stop();
  }
}
