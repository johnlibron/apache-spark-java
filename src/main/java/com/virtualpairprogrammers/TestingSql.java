package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class TestingSql {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.testing.memory", "471859200").getOrCreate();

        // spark.conf().set("spark.sql.shuffle.partitions", "2");

        Dataset<Row> ds = spark.read().option("header", true).csv("src/main/resources/logging/biglog.txt");

        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");

        spark.udf().register("monthNum", (String month) -> {
            Date inputDate = input.parse(month);
            return Integer.parseInt(output.format(inputDate));
        }, DataTypes.IntegerType);

        ds.createOrReplaceTempView("logging_table");

        // Dataset<Row> loggingResults = spark.sql("select level, collect_list(datetime) from logging_table group by level order by level");
        Dataset<Row> loggingResults = spark.sql(
                "select level, " +
                        "date_format(datetime, 'MMMM') as month, " +
                        "date_format(datetime, 'M') as month_num, " +
//                        "first(date_format(datetime, 'M')) as month_num, " +
                        "count(1) as total " +
                        "from logging_table " +
                        "group by level, month, month_num " +
//                        "order by monthNum(month), level" +
                        "order by cast(month_num as int), level");
         loggingResults = loggingResults.drop("month_num");

        // spark.sqlContext().dropTempTable("logging_table");
        // loggingResults.createOrReplaceTempView("logging_table");

        loggingResults.show(100);

        loggingResults.explain();

        // loggingResults.createOrReplaceTempView("results_table");
        // Dataset<Row> sumResults = spark.sql("select sum(total) from results_table");
        // sumResults.show();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();
    }
}
