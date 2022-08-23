package com.virtualpairprogrammers.rddv2.sumOfNumbers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SumOfNumbers {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer sumOfNumbers = sc.textFile("src/main/resources/in/prime_nums.text")
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(number -> !number.isEmpty())
                .map(Integer::valueOf)
                .reduce(Integer::sum);

        System.out.println("Sum is: " + sumOfNumbers);

        sc.close();
    }
}
