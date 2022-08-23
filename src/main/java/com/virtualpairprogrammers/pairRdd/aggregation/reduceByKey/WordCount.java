package com.virtualpairprogrammers.pairRdd.aggregation.reduceByKey;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Long> wordCounts = sc.textFile("src/main/resources/in/word_count.text")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum);

        Map<String, Long> worldCountsMap = wordCounts.collectAsMap();

        for (Map.Entry<String, Long> wordCountPair : worldCountsMap.entrySet()) {
            System.out.println(wordCountPair.getKey() + " : " + wordCountPair.getValue());
        }

        sc.close();
    }
}
