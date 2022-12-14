package com.virtualpairprogrammers.sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LogStreamAnalysis {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(30));

        JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);

        JavaDStream<String> results = inputData.map(item -> item);
        JavaPairDStream<String, Long> pairDStream = results
                .mapToPair(rawLogMessage -> new Tuple2<>(rawLogMessage.split(",")[0], 1L))
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(2));
        pairDStream.print();

        sc.start();

        sc.awaitTermination();

        sc.close();
    }
}
