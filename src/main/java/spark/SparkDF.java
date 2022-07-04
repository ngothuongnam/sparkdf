package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDF {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Dataframe Demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().getOrCreate();

        //create Spark Dataframe from csv file
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("./input/flights.csv");

        //add the HOUR_ARR field to calculate the number of flight hours
        Dataset<Row> df1 = df.withColumn("HOUR_ARR", df.col("AIR_TIME").divide(60));

        //calculate the average speed (km/h) of each flight
        df1.selectExpr("ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
                "TAIL_NUMBER", "(DISTANCE/HOUR_ARR) AS AVG_SPEED").show();

        //filter flights from SEA to ANC
        df1.filter("ORIGIN_AIRPORT == 'SEA'").filter("DESTINATION_AIRPORT == 'ANC'").show();

        //calculate average flight hours from ORIGIN_AIRPORT to DESTINATION_AIRPORT
        df1.groupBy("ORIGIN_AIRPORT", "DESTINATION_AIRPORT").avg("HOUR_ARR").show();
    }
}