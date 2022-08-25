package com.virtualpairprogrammers.sparkSql.dataset;

import com.virtualpairprogrammers.common.Utils;
import com.virtualpairprogrammers.sparkSql.Response;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class RddDatasetConversion {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("RddDatasetConversion").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder()
                .appName("StackOverFlowSurvey").master("local[1]")
                /*
                // Performance Tuning
                .config("spark.sql.codegen", false) (involves large queries or with the same repeated query)
                .config("spark.sql.inMemoryColumnarStorage.batchSize", 10000) (default is 1000, having a larger batch size can
                        improve memory utilization and compression, a batch with large number of records - 100 columns -
                        might be hard to build up in memory and can lead to an OutOfMemoryError, pick up a small batch size for that)

                // Should check these details https://level-up.one/avoid-these-mistakes-while-writing-apache-spark-program/
                .config("spark.dynamicAllocation.enabled", true)
                .coalesce() // reducing the partition size
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "128m")
                .set("spark.kryoserializer.buffer", "64m")
                .registerKryoClasses(Array(classOf[ArrayBuffer[String]], classOf[ListBuffer[String]]))
                */
                .getOrCreate();

        JavaRDD<Response> responseRDD = sc
                .textFile("src/main/resources/in/2016-stack-overflow-survey-responses.csv")
                .filter(response -> !response.split(Utils.COMMA_DELIMITER, -1)[2].equals("country"))
                .map(response -> {
                    String[] splits = response.split(Utils.COMMA_DELIMITER, -1);
                    return new Response(splits[2], toInt(splits[6]), splits[9], toInt(splits[14]));
                }).cache(); // Performance Tuning

        Dataset<Response> responseDataset = session.createDataset(responseRDD.rdd(), Encoders.bean(Response.class));

        System.out.println("=== Print out schema ===");
        responseDataset.printSchema();

        System.out.println("=== Print 20 records of responses table ===");
        responseDataset.show(20);

        JavaRDD<Response> responseJavaRDD = responseDataset.toJavaRDD();

        for (Response response : responseJavaRDD.collect()) {
            System.out.println(response);
        }

        session.close();
        sc.close();
    }
    private static Integer toInt(String split) {
        return split.isEmpty() ? null : Math.round(Float.parseFloat(split));
    }
}
