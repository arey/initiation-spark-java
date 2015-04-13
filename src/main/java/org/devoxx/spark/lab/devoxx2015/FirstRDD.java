package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FirstRDD {

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Path path = Paths.get(FirstRDD.class.getResource("/ratings.txt").getPath());
        JavaRDD<String> lines = sc.textFile(path.toString());
        System.out.println("Lines count: " + lines.count());
    }
}