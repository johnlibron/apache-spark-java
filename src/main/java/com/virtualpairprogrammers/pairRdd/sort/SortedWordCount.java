package com.virtualpairprogrammers.pairRdd.sort;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SortedWordCount {

    /* Create a Spark program to read an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
    */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Long> wordCounts = sc.textFile("src/main/resources/in/word_count.text")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .filter(word -> !word.isEmpty())
                .map(String::toLowerCase)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);

        for (Tuple2<String, Long> wordCount : wordCounts.collect()) {
            System.out.println(wordCount._1() + " : " + wordCount._2());
        }

        sc.close();
    }
}

