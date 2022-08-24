package com.virtualpairprogrammers.pairRdd.aggregation.combineByKey;

import com.virtualpairprogrammers.pairRdd.aggregation.AvgCount;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class AverageHousePrice {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it.

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria-Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Three types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("averageHousePrice").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Double> housePriceAvg = sc.textFile("src/main/resources/in/RealEstate.csv")
                .filter(line -> !line.contains("Bedrooms"))
                .mapToPair(line -> {
                    String[] splits = line.split(",");
                    return new Tuple2<>(splits[3], Double.parseDouble(splits[2]));
                })
                .combineByKey(
                        price -> new AvgCount(1, price),
                        (prevAvgCount, currentPrice) -> new AvgCount(prevAvgCount.getCount() + 1,
                                prevAvgCount.getTotal() + currentPrice),
                        (avgCountA, avgCountB) -> new AvgCount(avgCountA.getCount() + avgCountB.getCount(),
                                avgCountA.getTotal() + avgCountB.getTotal()))
                .mapValues(avgCount -> avgCount.getTotal() / avgCount.getCount());

        for (Map.Entry<String, Double> housePriceAvgPair : housePriceAvg.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
        }

        sc.close();
    }
}
