package com.virtualpairprogrammers.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class PersistExample {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("persist").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> integerRdd = sc.parallelize(inputIntegers);

        integerRdd.persist(StorageLevel.MEMORY_ONLY());

        Integer product = integerRdd.reduce((x, y) -> x * y);

        long count = integerRdd.count();

        Integer sum = integerRdd.reduce(Integer::sum);

        System.out.println("product = " + product);
        System.out.println("count = " + count);
        System.out.println("sum = " + sum);

        sc.close();
    }
}
