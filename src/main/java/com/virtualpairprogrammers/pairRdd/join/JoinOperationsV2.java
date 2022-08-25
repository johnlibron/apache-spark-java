package com.virtualpairprogrammers.pairRdd.join;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinOperationsV2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<>(4, 18));
		visitsRaw.add(new Tuple2<>(6, 4));
		visitsRaw.add(new Tuple2<>(10, 9));

		List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<>(1, "John"));
		usersRaw.add(new Tuple2<>(2, "Bob"));
		usersRaw.add(new Tuple2<>(3, "Alan"));
		usersRaw.add(new Tuple2<>(4, "Doris"));
		usersRaw.add(new Tuple2<>(5, "Marybelle"));
		usersRaw.add(new Tuple2<>(6, "Raquel"));

		JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users);

		joinedRdd.collect().forEach(tuple -> System.out.println("User " + tuple._2._2 + " had " + tuple._2._1.orElse(0)));

		sc.close();
	}
}
