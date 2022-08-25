package com.virtualpairprogrammers.sparkSql.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class TestingSqlDataFrames {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/logging/biglog.txt");

        Object[] months = new Object[] {
            "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"
        };
        List<Object> columns = Arrays.asList(months);

        dataset = dataset.select(
                col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("month_num").cast(DataTypes.IntegerType))
            .groupBy(
                col("level"),
                col("month"),
                col("month_num")).count().as("total")
            .orderBy(
                col("month_num"),
                col("level"))
            .drop(
                col("month_num"));

//        dataset = dataset.groupBy(col("level")).pivot(col("month"), columns).count().na().fill(0);
        dataset.show(100);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();
    }
}
