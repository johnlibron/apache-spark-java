package com.virtualpairprogrammers.pairRdd.groupByKey;

import com.virtualpairprogrammers.common.Utils;
import org.apache.commons.collections.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AirportsByCountry {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Iterable<String>> airportsByCountryWithGroupByKey = sc
                .textFile("src/main/resources/in/airports.text")
                .mapToPair(airport -> {
                    String[] splits = airport.split(Utils.COMMA_DELIMITER);
                    return new Tuple2<>(splits[3], splits[1]);
                })
                .groupByKey();

        for (Map.Entry<String, Iterable<String>> airports : airportsByCountryWithGroupByKey.collectAsMap().entrySet()) {
            System.out.println(airports.getKey() + " : " + airports.getValue());
        }

        JavaPairRDD<String, List<String>> airportsByCountryWithReduceByKey = sc
                .textFile("src/main/resources/in/airports.text")
                .mapToPair(airport -> {
                    String[] splits = airport.split(Utils.COMMA_DELIMITER);
                    return new Tuple2<>(splits[3], Collections.singletonList(splits[1]));
                })
                .reduceByKey(ListUtils::union);

        for (Map.Entry<String, List<String>> airports : airportsByCountryWithReduceByKey.collectAsMap().entrySet()) {
            System.out.println(airports.getKey() + " : " + airports.getValue());
        }

        sc.close();
    }
}
