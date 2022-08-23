package com.virtualpairprogrammers.pairRdd.filter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsNotInUsa {

    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in the United States and
           output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> airportsNotInUSA = sc.textFile("src/main/resources/in/airports.text")
                .mapToPair(line -> {
                    String[] splits = line.split(COMMA_DELIMITER);
                    return new Tuple2<>(splits[1], splits[3]);
                })
                .filter(row -> !row._2.equals("\"United States\""));
        airportsNotInUSA.collect().forEach(System.out::println);
//        airportsNotInUSA.saveAsTextFile("src/main/resources/out/airports_not_in_usa_pair_rdd.text");

        sc.close();
    }
}
