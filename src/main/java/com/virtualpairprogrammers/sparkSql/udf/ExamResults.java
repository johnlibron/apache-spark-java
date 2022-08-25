package com.virtualpairprogrammers.sparkSql.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class ExamResults {

    private static final UDF2<String, String, Boolean> hasPassedFunction = (grade, subject) -> {
        if (subject.equals("Biology")) {
            return grade.startsWith("A");
        }
        return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
    };

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

//        .option("inferSchema", true)
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        /*
        dataset = dataset.groupBy(col("subject"))
            .agg(max(col("score")).alias("max score"),
                min(col("score")).alias("min score"));
        */

        /*
        dataset = dataset.groupBy(col("subject")).pivot(col("year"))
                .agg(round(avg(col("score")), 2).alias("avg score"),
                        round(stddev(col("score")), 2).alias("stddev score"));
        */

        spark.udf().register("hasPassed", hasPassedFunction, DataTypes.BooleanType);

        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));
        dataset.show();

        spark.close();
    }
}
