package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Requête SQL sur un DataFrame chargé depuis un fichier JSON.
 */
public class Workshop3 {

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        String path = Workshop3.class.getResource("/products.json").getPath();
        DataFrame products = sqlContext.load(path, "json");
        System.out.println(products.first());

        sqlContext.registerDataFrameAsTable(products, "products");
        DataFrame frame = sqlContext.sql("SELECT count(*) FROM products where id > 999");
        System.out.println(frame.first());
    }
}
