package com.virtualpairprogrammers.advanced.accumulator;

import com.virtualpairprogrammers.common.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

public class StackOverFlowSurvey {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");
        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();
        final LongAccumulator processedBytes = new LongAccumulator();

        total.register(sparkContext, Option.apply("total"), false);
        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle point"), false);
        processedBytes.register(sparkContext, Option.apply("Processed bytes"), true);

        JavaRDD<String> responseFromCanada = javaSparkContext
                .textFile("src/main/resources/in/2016-stack-overflow-survey-responses.csv")
                .filter(response -> {
                    processedBytes.add(response.getBytes().length);
                    String[] splits = response.split(Utils.COMMA_DELIMITER, -1);
                    total.add(1);
                    if (splits[14].isEmpty()) {
                        missingSalaryMidPoint.add(1);
                    }
                    return splits[2].equals("Canada");
                });

        System.out.println("Count of responses from Canada: " + responseFromCanada.count());
        System.out.println("Number of bytes processed: " + processedBytes.value());
        System.out.println("Total count of responses: " + total.value());
        System.out.println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());

        javaSparkContext.close();
    }
}
