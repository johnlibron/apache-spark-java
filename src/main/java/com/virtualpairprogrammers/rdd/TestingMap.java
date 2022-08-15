package com.virtualpairprogrammers.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TestingMap {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        conf.set("spark.testing.memory", "471859200");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Transformation
        JavaPairRDD<Long, String> sorted = sc.textFile("src/main/resources/subtitles/input.txt")
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()) // lettersOnlyRdd
                .filter(sentence -> sentence.trim().length() > 0) // removedBlankLines
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator()) // justWords
                .filter(word -> word.trim().length() > 0) // blankWordsRemoved
                .filter(Util::isNotBoring) // justInterestingWords
                .mapToPair(word -> new Tuple2<>(word, 1L)) // pairRDD
                .reduceByKey(Long::sum) // totals
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)) // switched
                .sortByKey(false);

        System.out.println("There are " + sorted.getNumPartitions() + " partitions.");

        List<Tuple2<Long, String>> results = sorted.take(100000); // ----> Action
        results.forEach(System.out::println);

        sc.close();
    }
}
