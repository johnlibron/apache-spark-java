package com.virtualpairprogrammers.rdd;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		conf.set("spark.testing.memory", "471859200");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData
			.mapToPair(row -> new Tuple2<>(row._2, 1))
			.reduceByKey(Integer::sum);

		viewData.distinct() // Remove any duplicated views
			// Get the courseIds into the RDD
			.mapToPair(row -> new Tuple2<>(row._2, row._1)) // chapterId, userId
			.join(chapterData) // chapterId, (userId, courseId)
			// Don't need chapterIds
			.mapToPair(row -> {
				Integer userId = row._2._1;
				Integer courseId = row._2._2;
				return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
			}) // (userId, courseId), count
			// Count how many views for each user per course
			.reduceByKey(Long::sum) // (userId, courseId), views
			// Remove the userIds
			.mapToPair(row -> {
				Integer courseId = row._1._2;
				Long views = row._2;
				return new Tuple2<>(courseId, views);
			}) // courseId, views
			// Add in the total chapter count
			.join(chapterCountRdd) // courseId, (views, of)
			// Convert to percentage
			.mapValues(value -> (double) value._1 / value._2) // courseId, percentage
			// Convert to scores
			.mapValues(value -> {
				if (value > 0.9) return 10L;
				if (value > 0.5) return 4L;
				if (value > 0.25) return 2L;
				return 0L;
			}) // courseId, score
			.reduceByKey(Long::sum) // courseId, totalScore
			.join(titlesData) // courseId, (totalScore, title)
			.mapToPair(row -> {
				Long totalScore = row._2._1;
				Integer courseId = row._1;
				String title = row._2._2;
				return new Tuple2<>(totalScore, new Tuple2<>(courseId, title));
			}) // totalScore, (courseId, title)
			.sortByKey(false) // sort descending total score
			.take(1000).forEach(System.out::println);

		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<>(Integer.valueOf(cols[0]), cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
													  	}).cache();
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-1.csv," +
								"src/main/resources/viewing figures/views-2.csv," +
								"src/main/resources/viewing figures/views-3.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<>(Integer.valueOf(columns[0]), Integer.valueOf(columns[1]));
				     });
	}
}
