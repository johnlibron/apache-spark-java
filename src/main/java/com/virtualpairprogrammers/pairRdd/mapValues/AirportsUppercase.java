package com.virtualpairprogrammers.pairRdd.mapValues;

import com.virtualpairprogrammers.common.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsUppercase {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
           being the key and country name being the value. Then convert the country name to uppercase and
           output the pair RDD to out/airports_uppercase.text

           Each row of the input file contains the following columns:

           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "CANADA")
           ("Wewak Intl", "PAPUA NEW GUINEA")
           ...
         */
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> airportsCountryUppercase = sc.textFile("src/main/resources/in/airports.text")
                .mapToPair(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return new Tuple2<>(splits[1], splits[3]);
                })
                .mapValues(String::toUpperCase);
        airportsCountryUppercase.collect().forEach(System.out::println);
//        airportsCountryUppercase.saveAsTextFile("src/main/resources/out/airports_uppercase_pair_rdd.text");

        sc.close();
    }
}
