package com.virtualpairprogrammers.rdd.airport;

import com.virtualpairprogrammers.common.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsa {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,
           find all the airports which are located in the United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportsNameAndCityNames = sc.textFile("src/main/resources/in/airports.text")
                .filter(airport -> airport.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""))
                .map(airport -> {
                    String[] splits = airport.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{ splits[1], splits[2] }, ",");
                });
        airportsNameAndCityNames.collect().forEach(System.out::println);
//        airportsNameAndCityNames.saveAsTextFile("src/main/resources/out/airports_in_usa.text");

        sc.close();
    }
}
