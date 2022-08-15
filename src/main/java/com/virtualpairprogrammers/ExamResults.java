package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;

public class ExamResults {

    private static final UDF2<String, String, Boolean> hasPassedFunction = (grade, subject) -> {
        if (subject.equals("Biology")) {
            return grade.startsWith("A");
        }
        return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
    };

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.testing.memory", "471859200").getOrCreate();

//        .option("inferSchema", true)
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

//        dataset = dataset.groupBy(col("subject"))
//                .agg(max(col("score")).alias("max score"),
//                    min(col("score")).alias("min score"));

//        dataset = dataset.groupBy(col("subject")).pivot(col("year"))
//                .agg(round(avg(col("score")), 2).alias("avg score"),
//                        round(stddev(col("score")), 2).alias("stddev score"));
//
        spark.udf().register("hasPassed", hasPassedFunction, DataTypes.BooleanType);

        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));
        dataset.show();

        spark.close();
    }
}
