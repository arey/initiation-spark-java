package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;

/**
 * Calcule la moyenne, le min, le max et le nombre de votes de l'utilisateur n°200.
 */
public class Workshop1 {

    public void run() throws URISyntaxException {
        SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String ratingsPath = Paths.get(getClass().getResource("/ratings.txt").getPath()).toString();

        JavaRDD<Rating> ratings = sc.textFile(ratingsPath)
                .map(line -> line.split("\\t"))
                .map(row -> new Rating(
                        Long.parseLong(row[0]),
                        Long.parseLong(row[1]),
                        Integer.parseInt(row[2]),
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(row[3]) * 1000), ZoneId.systemDefault())

                ));

        double mean = ratings
                .filter(rating -> rating.user == 200)
                .mapToDouble(rating -> rating.rating)
                .mean();

        double max = ratings
                .filter(rating -> rating.user == 200)
                .mapToDouble(rating -> rating.rating)
                .max(Comparator.<Double>naturalOrder());

        double min = ratings
                .filter(rating -> rating.user == 200)
                .mapToDouble(rating -> rating.rating)
                .min(Comparator.<Double>naturalOrder());

        double count = ratings
                .filter(rating -> rating.user == 200)
                .count();


        System.out.println("mean: " + mean);
        System.out.println("max: " + max);
        System.out.println("min: " + min);
        System.out.println("count: " + count);

    }


    public static void main(String... args) throws URISyntaxException {
        new Workshop1().run();
    }
}
