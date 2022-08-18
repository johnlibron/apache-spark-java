package com.virtualpairprogrammers.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestingSqlDatasets {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.testing.memory", "471859200").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show();

        long numberOfRows = dataset.count();
        System.out.println("There are " + numberOfRows + " records");

        Row rowFirst = dataset.first();

        String subject = rowFirst.getString(2);
        int year = Integer.parseInt(rowFirst.getAs("year"));
        System.out.println(subject);
        System.out.println(year);

//        Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");
        Dataset<Row> modernArtResults = dataset.filter(
                subjectColumn.equalTo("Modern Art")
                .and(yearColumn.geq(2007)));

        modernArtResults.show();
        System.out.println("There are " + modernArtResults.count() + " records");

        dataset.createOrReplaceTempView("students_table");

        Dataset<Row> results = spark.sql("select distinct year from students_table order by year desc");
        results.show();

        spark.close();
    }
}
