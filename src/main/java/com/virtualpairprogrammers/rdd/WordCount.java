package com.virtualpairprogrammers.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, Long> wordCounts = sc.textFile("src/main/resources/in/word_count.text")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .countByValue();

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

        sc.close();
    }
}
