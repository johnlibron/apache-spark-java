package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestingIntegers {

    public static void main(String[] args) {
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);

        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        conf.set("spark.testing.memory", "471859200");
        JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

		Integer result = originalIntegers.reduce(Integer::sum);

//		JavaRDD<Double> sqrtRdd = originalIntegers.map(Math::sqrt);

		JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(IntegerWithSquareRoot::new);

		JavaRDD<Tuple2<Integer, Double>> sqrtRdd1 = originalIntegers.map( value -> new Tuple2<>(value, Math.sqrt(value)) );

		sqrtRdd1.collect().forEach( System.out::println );

		System.out.println(result);

        // how many elements in sqrtRdd
        // using just map and reduce
		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L);
		Long count = singleIntegerRdd.reduce(Long::sum);
		System.out.println(count);

        sc.close();
    }
}
