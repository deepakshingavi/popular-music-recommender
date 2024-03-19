package com.dp.example;

import com.dp.example.processor.DataProcessor;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class StartMusicRecommenderTest {

    private static SparkSession spark;
    private static DataProcessor dataProcessor;

    private static String path = "src/test/resources/userid-timestamp-artid-artname-traid-traname_small.tsv";

    @BeforeClass
    public static void setUp() {
        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Test-MusicRecommender")
                .getOrCreate();
        dataProcessor = new DataProcessor(spark);
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }
}
