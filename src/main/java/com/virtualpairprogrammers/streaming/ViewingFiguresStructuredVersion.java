package com.virtualpairprogrammers.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class ViewingFiguresStructuredVersion {

    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkSession sparkSession = SparkSession.builder().appName("structuredViewingReport").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("WARN");

        sparkSession.conf().set("spark.sql.shuffle.partitions", "10");

        Dataset<Row> df = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        df.createOrReplaceTempView("viewing_figures");

        Dataset<Row> results = sparkSession
                .sql("select window, cast(value as string) as course_name, " +
                        "sum(5) as seconds_watched " +
                        "from viewing_figures " +
                        "group by window(timestamp, '2 minutes'), course_name ");

        StreamingQuery query = results.writeStream().format("console")
                .outputMode(OutputMode.Update())
                .option("truncate", false)
                .option("numRows", 50)
                .start();

        query.awaitTermination();

        sparkSession.close();

    }
}
