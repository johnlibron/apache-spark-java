package com.virtualpairprogrammers.pairRdd.groupByKey;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Iterables;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GroupByKeyVsReduceByKey {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> words = Arrays.asList("one", "two", "two", "three", "three", "three");
        JavaPairRDD<String, Integer> wordsPairRdd = sc.parallelize(words).mapToPair(word -> new Tuple2<>(word, 1));

        List<Tuple2<String, Integer>> wordCountsWithReduceByKey = wordsPairRdd.reduceByKey(Integer::sum).collect();
        System.out.println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey);

        List<Tuple2<String, Integer>> wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(Iterables::size).collect();
        System.out.println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey);

        sc.close();
    }
}

