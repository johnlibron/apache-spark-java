package com.virtualpairprogrammers.pairRdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddFromRegularRdd {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputStrings = Arrays.asList("Lily 23", "Jack 29", "Mary 29", "James 8");

        JavaRDD<String> regularRDDs = sc.parallelize(inputStrings);

        JavaPairRDD<String, Integer> pairRDD = regularRDDs.mapToPair(input -> {
            String[] splits = input.split(" ");
            return new Tuple2<>(splits[0], Integer.valueOf(splits[1]));
        });

        pairRDD.coalesce(1).collect().forEach(System.out::println);
//        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");

        sc.close();
    }
}
