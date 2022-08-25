package com.virtualpairprogrammers.rdd.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CollectExample {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
        JavaRDD<String> wordRdd = sc.parallelize(inputWords);

        List<String> words = wordRdd.collect();

        for (String  word : words) {
            System.out.println(word);
        }

        sc.close();
    }
}
