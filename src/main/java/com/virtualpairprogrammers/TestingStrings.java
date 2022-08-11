package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestingStrings {

    public static void main(String[] args) {

		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        conf.set("spark.testing.memory", "471859200");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

		sc.parallelize(inputData)
				.flatMap(value -> Arrays.stream(value.split(" ")).iterator())
				.filter(word -> word.length() > 1)
				.collect().forEach(System.out::println);

		sc.parallelize(inputData)
				.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
				.reduceByKey(Long::sum)
				.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

		JavaPairRDD<String, String> pairRDD = originalLogMessages.mapToPair(rawValue -> {
			String[] columns = rawValue.split(":");
			String level = columns[0];
			String date = columns[1].trim();
			return new Tuple2<>(level, date);
		});

		pairRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
