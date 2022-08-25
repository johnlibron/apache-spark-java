package com.virtualpairprogrammers.pairRdd.groupByKey;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.sparkproject.guava.collect.Iterables;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GroupByKeyVsReduceByKey {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> words = Arrays.asList("one", "two", "two", "three", "three", "three");
        JavaPairRDD<String, Integer> wordsPairRdd = sc.parallelize(words).mapToPair(word -> new Tuple2<>(word, 1));

        List<Tuple2<String, Integer>> wordCountsWithReduceByKey = wordsPairRdd.reduceByKey(Integer::sum).collect();
        System.out.println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey);

        List<Tuple2<String, Integer>> wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(Iterables::size).collect();
        System.out.println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey);

        JavaPairRDD<String, Integer> partitionedWordPairRDD = wordsPairRdd.partitionBy(new HashPartitioner(4));
        partitionedWordPairRDD.persist(StorageLevel.DISK_ONLY());
        // partitionedWordPairRDD.groupByKey().mapToPair(word -> new Tuple2<>(word._1, getSum(word._2))).collect();

        sc.close();
    }
}

