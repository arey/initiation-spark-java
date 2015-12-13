package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FirstRDD {

    public static void main(String... args) {
        FirstRDD runner = new FirstRDD();
        SparkConf conf = new SparkConf().setAppName("Sparky CDN").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Path path = runner.getFilePath(runner.getFile());
        JavaRDD<String> lines = sc.textFile(path.toString());
        System.out.println("Lines count: " + lines.count());
    }

    private File getFile() {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource("ratings.txt").getFile());
    }

    private Path getFilePath(File file) {
        Path path = Paths.get(file.toURI());
        return path;
    }
}