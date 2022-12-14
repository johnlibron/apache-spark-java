package com.virtualpairprogrammers.sparkSql.join;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;

public class UkMakerSpaces {

    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate();

        Dataset<Row> makerSpace = session.read().option("header", "true")
                .csv("src/main/resources/in/uk-makerspaces-identifiable-data.csv");

        Dataset<Row> postCode = session.read().option("header", "true")
                .csv("src/main/resources/in/uk-postcode.csv")
                .withColumn("PostCode", concat_ws("", col("PostCode"), lit(" ")));

        System.out.println("=== Print 20 records of makerspace table ===");
        makerSpace.show();

        System.out.println("=== Print 20 records of postcode table ===");
        postCode.show();

        Dataset<Row> joined = makerSpace.join(postCode,
                makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer");

        System.out.println("=== Print 20 records of joined table ===");
        joined.show();

        System.out.println("=== Group by Region ===");
        joined.groupBy("Region").count().show();
    }
}