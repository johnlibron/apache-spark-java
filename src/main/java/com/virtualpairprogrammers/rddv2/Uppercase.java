package com.virtualpairprogrammers.rddv2;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Uppercase {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lowerCaseLines = sc.textFile("src/main/resources/in/uppercase.text")
                .map(String::toUpperCase);

        lowerCaseLines.collect().forEach(System.out::println);
//        lowerCaseLines.saveAsTextFile("src/main/resources/out/uppercase.text");

        sc.close();
    }
}
