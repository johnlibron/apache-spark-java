package com.virtualpairprogrammers.rddv2.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHosts {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
        SparkConf conf = new SparkConf().setAppName("intersectionLogs").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("src/main/resources/in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sc.textFile("src/main/resources/in/nasa_19950801.tsv");

        JavaRDD<String> julyFirstHosts = julyFirstLogs.map(line -> line.split("\t")[0]);
        JavaRDD<String> augustFirstHosts = augustFirstLogs.map(line -> line.split("\t")[0]);

        JavaRDD<String> intersection = julyFirstHosts.intersection(augustFirstHosts)
                .filter(line -> !(line.startsWith("host") && line.endsWith("bytes")));

        intersection.collect().forEach(System.out::println);
//        intersection.saveAsTextFile("out/nasa_logs_same_hosts.csv");

        sc.close();
    }
}
