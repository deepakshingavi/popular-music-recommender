package com.dp.example;

import com.dp.example.processor.DataProcessor;
import com.dp.example.model.UserLog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class StartMusicRecommender {

    public static void main(String[] args) {
        long startTime = System.nanoTime();
        if (args.length < 4) {
            System.err.println("Error : Insufficient argument provided.");
            System.exit(1);
        }

        final Map<String, String> argsMap = parseArgs(args);

        if(!argsMap.containsKey("input") || argsMap.get("input").trim().isEmpty()) {
            throw new RuntimeException("--input program argument not provided");
        }
        if(!argsMap.containsKey("output") || argsMap.get("output").trim().isEmpty()) {
            throw new RuntimeException("--output program argument not provided");
        }

        String inputDir = argsMap.get("input");
        String outputDir = argsMap.get("output");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("PopularMusicRecommender")
                .getOrCreate();

        final Dataset<Row> rankedByTracksDF = getRecommendations(spark, inputDir);

        rankedByTracksDF
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .option("delimiter",DataProcessor.CSV_FIELD_DELIMITER)
                .csv(outputDir);

        spark.stop();

        //     Capture the end time
        long endTime = System.nanoTime();

        // Calculate the execution time in milliseconds
        long executionTime = (endTime - startTime) / 1000000;

        System.out.println("Execution time: " + executionTime + " milliseconds");
    }

    public static Dataset<Row> getRecommendations(SparkSession spark, String inputDir) {

        final DataProcessor dataProcessor = new DataProcessor(spark);

        Dataset<Row> userSessionLogs = dataProcessor.loadData(inputDir);

        Dataset<UserLog> userLogDS = dataProcessor.mapToBean(userSessionLogs);

        Dataset<Row> dsWithSessionIds =  dataProcessor.tagWithSessionId(userLogDS);

        Dataset<Row> rankedSession =  dataProcessor.getTopSession(dsWithSessionIds,50);

        Dataset<Row> rankedByTracksDF = dataProcessor.getTopPlayedTracks(rankedSession,dsWithSessionIds, 10);

        return rankedByTracksDF;
    }

    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> resultMap = new HashMap<>();
        for (int i = 0; i < args.length - 1; i += 2) {
            String key = args[i].replaceFirst("--", "");
            String value = args[i + 1];
            resultMap.put(key, value);
        }
        return resultMap;
    }


}