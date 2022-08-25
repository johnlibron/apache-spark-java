package com.virtualpairprogrammers.sparkSql.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;

public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
            Collections.singletonList(person)
        , personEncoder);
        javaBeanDS.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> primitiveDS = spark.createDataset(
            Arrays.asList(1L, 2L, 3L)
        , longEncoder);
        Dataset<Long> transformDS = primitiveDS.map(
            (MapFunction<Long, Long>) value -> value + 1L
        , longEncoder);
        System.out.println(transformDS.collectAsList());

        // ******************************************************************************************

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        StructType schema = new StructType(new StructField[] {
            new StructField("student_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("exam_center_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("subject", DataTypes.StringType, false, Metadata.empty()),
            new StructField("year", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("quarter", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("score", DataTypes.LongType, false, Metadata.empty()),
            new StructField("grade", DataTypes.StringType, false, Metadata.empty())
        });
        Encoder<Student> studentEncoder = Encoders.bean(Student.class);
        Dataset<Student> studentDataset = spark.read().option("header", true)
            .schema(schema).csv("src/main/resources/exams/students.csv")
            .withColumnRenamed("student_id", "studentId")
            .withColumnRenamed("exam_center_id", "examCenterId")
            .as(studentEncoder);
        studentDataset.printSchema();
        studentDataset.show();

        // ******************************************************************************************

        // Create an RDD of Person objects from a text file
        JavaRDD<Log> logRDD = spark.read()
            .textFile("src/main/resources/logging/biglog-*.txt")
            .javaRDD()
            .map(line -> {
                String[] parts = line.split(",");
                Log log = new Log();
                log.setLevel(parts[0]);
                log.setDatetime(parts[1]);
                return log;
            });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> logDF = spark.createDataFrame(logRDD, Log.class);
        // Register the DataFrame as a temporary view
        logDF.createOrReplaceTempView("log");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> warnDF = spark.sql("SELECT datetime FROM log WHERE level = 'WARN'");

        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> datetimeWarnLevelDF = warnDF.map(
            (MapFunction<Row, String>) row -> "Datetime: " + row.getAs("datetime")
        , stringEncoder);
        datetimeWarnLevelDF.show(false);

    }
}
