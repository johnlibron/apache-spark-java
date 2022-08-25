package com.virtualpairprogrammers.pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.sparkproject.guava.collect.Iterables;
import scala.Tuple2;

import java.util.Scanner;

public class PartitionTesting {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/loggingV2/bigLog-*.txt");

		System.out.println("Initial RDD Partition Size: " + initialRdd.getNumPartitions());
		
		JavaPairRDD<String, String> warningsAgainstDate = initialRdd
				.filter(line -> !line.isEmpty())
				.mapToPair(inputLine -> {
					String[] cols = inputLine.split(":");
					String level = cols[0];
					String date = cols[1];
					return new Tuple2<>(level, date);
				});
		
		System.out.println("After a narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");
		
		// Now we're going to do a "wide" transformation
		JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();
		
		results = results.persist(StorageLevel.MEMORY_AND_DISK());
		
		System.out.println(results.getNumPartitions() + " partitions after the wide transformation");
		
		results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));
		
		System.out.println(results.count());
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		sc.close();
	}
}