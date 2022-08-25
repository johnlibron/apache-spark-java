package com.virtualpairprogrammers.sparkSql.dataset;

import com.virtualpairprogrammers.sparkSql.Response;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;


public class TypedDataset {
    private static final String AGE_MIDPOINT = "ageMidpoint";
    private static final String SALARY_MIDPOINT = "salaryMidPoint";
    private static final String SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket";

    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder().appName("typedDataset").master("local[*]").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        Dataset<Row> responses = dataFrameReader.option("header","true")
                .csv("src/main/resources/in/2016-stack-overflow-survey-responses.csv");

        Dataset<Row> responseWithSelectedColumns = responses.select(
                col("country"),
                col("age_midpoint").as(AGE_MIDPOINT).cast("integer"),
                col("occupation"),
                col("salary_midpoint").as(SALARY_MIDPOINT).cast("integer"));

        Dataset<Response> typedDataset = responseWithSelectedColumns.as(Encoders.bean(Response.class));

        System.out.println("=== Print out schema ===");
        typedDataset.printSchema();

        System.out.println("=== Print 20 records of responses table ===");
        typedDataset.show(20);

        System.out.println("=== Print the responses from Afghanistan ===");
        typedDataset.filter((FilterFunction<Response>) response ->
                response.getCountry().equals("Afghanistan")).show();

        System.out.println("=== Print the count of occupations ===");
        typedDataset.groupBy(typedDataset.col("occupation")).count().show();

        System.out.println("=== Print responses with average mid age less than 20 ===");
        typedDataset.filter((FilterFunction<Response>) response ->
                response.getAgeMidPoint() != null && response.getAgeMidPoint() < 20).show();

        System.out.println("=== Print the result by salary middle point in descending order ===");
        typedDataset.orderBy(typedDataset.col(SALARY_MIDPOINT).desc()).show();

        System.out.println("=== Group by country and aggregate by average salary middle point and max age middle point ===");
        typedDataset.filter((FilterFunction<Response>) response -> response.getSalaryMidPoint() != null)
                .groupBy("country")
                .agg(avg(SALARY_MIDPOINT), max(AGE_MIDPOINT))
                .show();

        System.out.println("=== Group by salary bucket ===");
        typedDataset.map((MapFunction<Response, Integer>) response -> response.getSalaryMidPoint() == null ?
                    null : Math.round(response.getSalaryMidPoint() / 20000f) * 20000, Encoders.INT())
                .withColumnRenamed("value", SALARY_MIDPOINT_BUCKET)
                .groupBy(SALARY_MIDPOINT_BUCKET)
                .count()
                .orderBy(SALARY_MIDPOINT_BUCKET).show();
    }
}
