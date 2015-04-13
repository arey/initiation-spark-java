package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;

/**
 * Statistiques de repartition des notes de l'utilisateur 200 avec mis en oeuvre du cache Spark.
 */
public class Workshop2 {

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

        JavaRDD<Rating> cachedRatingsForUser = ratings
                .filter(rating -> rating.user == 200)
                .cache();

        double max = cachedRatingsForUser
                .mapToDouble(rating -> rating.user)
                .max(Comparator.<Double>naturalOrder());

        double count = cachedRatingsForUser
                .count();

        cachedRatingsForUser.unpersist(false);

        System.out.println("count: " + count);
        System.out.println("max: " + max);

    }


    public static void main(String[] args) throws URISyntaxException {
        Workshop2 workshop2 = new Workshop2();
        workshop2.run();
    }
}
