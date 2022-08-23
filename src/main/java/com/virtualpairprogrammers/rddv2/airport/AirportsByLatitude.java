package com.virtualpairprogrammers.rddv2.airport;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitude {

    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,
           find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportsNameAndLatitude = sc.textFile("src/main/resources/in/airports.text")
                .filter(line -> Double.parseDouble(line.split(COMMA_DELIMITER)[6]) > 40)
                .map(line -> {
                    String[] splits = line.split(COMMA_DELIMITER);
                    return StringUtils.join(new String[] {splits[1], splits[6]}, ",");
                });
        airportsNameAndLatitude.collect().forEach(System.out::println);
//        airportsNameAndLatitude.saveAsTextFile("src/main/resources/out/airports_by_latitude.text");

        sc.close();
    }
}
