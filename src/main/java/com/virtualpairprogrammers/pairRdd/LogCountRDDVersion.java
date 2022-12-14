package com.virtualpairprogrammers.pairRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static com.virtualpairprogrammers.pairRdd.LogCountRDDVersion.SerializableComparator.serialize;

public class LogCountRDDVersion {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, Long> pairs = sc.textFile("src/main/resources/logging/biglog-*.txt")
				.filter(line -> !line.startsWith("level,datetime"))
				.mapToPair(rawValue -> {
					String[] csvFields = rawValue.split(",");
					String level = csvFields[0];
					String date = csvFields[1];
					String month = rawDateToMonth(date);
					String key = level + ":" + month;
					return new Tuple2<>(key, 1L);
				})
				.reduceByKey(Long::sum);

		// order by
		Comparator<String> comparator = serialize((a,b) -> {
			String monthA = a.split(":")[1];
			String monthB = b.split(":")[1];
			return monthToMonthnum(monthA) - monthToMonthnum(monthB);
		});

		// assuming it is a stable sort, we can sort by secondary first (level) and then sort by primary (month).
		JavaPairRDD<String, Long> resultsRdd = pairs.sortByKey().sortByKey(comparator);
		
		List<Tuple2<String, Long>> results = resultsRdd.take(100);
		
		System.out.println("Level\tMonth\t\tTotal");
		for (Tuple2<String, Long> nextResult : results) {
			String[] levelMonth = nextResult._1.split(":");
			String level = levelMonth[0];
			String month = levelMonth[1];
			Long total = nextResult._2;
			System.out.println(level+"\t" + month + "\t\t" + total);
		}

		sc.close();
	}

	private static String rawDateToMonth(String raw) {
		SimpleDateFormat rawFmt = new SimpleDateFormat("yyyy-M-d hh:mm:ss");
		SimpleDateFormat requiredFmt = new SimpleDateFormat("MMMM");
		Date results;
		try {
			results = rawFmt.parse(raw);
			return requiredFmt.format(results);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static int monthToMonthnum(String month) {
		SimpleDateFormat rawFmt = new SimpleDateFormat("MMMM");
		SimpleDateFormat requiredFmt = new SimpleDateFormat("M");
		Date results;
		try {
			results = rawFmt.parse(month);
			return Integer.parseInt(requiredFmt.format(results));
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	public static interface SerializableComparator<T> extends Comparator<T>, Serializable {
		static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
			return comparator;
		}
	}
}



