package com.dp.example;

import com.dp.example.model.UserLog;
import com.dp.example.processor.DataProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class DataProcessorTest {
    private static SparkSession spark;
    private static DataProcessor dataProcessor;

    private static final String path = "src/test/resources/userid-timestamp-artid-artname-traid-traname_small.tsv";

    @BeforeClass
    public static void setUp() {
        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Test-PopularMusicRecommender")
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

    @Test
    public void loadDataTest() {
        final Dataset<Row> rowDataset = dataProcessor.loadData(path);
        assertTrue(rowDataset.count() > 0);
    }

    @Test
    public void loadMap() {
        final Dataset<Row> rowDataset = dataProcessor.loadData(path);
        final Dataset<UserLog> userLogDataset = dataProcessor.mapToBean(rowDataset);
        final List<UserLog> userLogList = userLogDataset.limit(3).collectAsList();
        for (UserLog log : userLogList) {
            assertNotNull(log);
            assertNotNull(log.getUserId());
            assertNotNull(log.getLoginTs());
            assertNotNull(log.getUserId());
            assertNotNull(log.getAritstName());
            assertNotNull(log.getArtistId());
            assertNotNull(log.getTrackName());
        }

    }

    @Test
    public void tagWithSessionIdTest() {
        final Dataset<Row> rowDataset = dataProcessor.loadData(path);
        final Dataset<UserLog> userLogDataset = dataProcessor.mapToBean(rowDataset);
        final Dataset<Row> dsWithSessionId = dataProcessor.tagWithSessionId(userLogDataset);
        final Set<String> sessionIds = dsWithSessionId.select("sessionId").distinct().collectAsList()
                .stream().map(row -> (String) row.getAs(0)).collect(Collectors.toSet());

        final Set<String> expectedSessionIds = new HashSet<>();
        expectedSessionIds.add("user_000001_1");
        expectedSessionIds.add("user_000002_1");
        expectedSessionIds.add("user_000003_1");
        expectedSessionIds.add("user_000004_1");
        expectedSessionIds.add("user_000005_1");
        expectedSessionIds.add("user_000006_1");
        expectedSessionIds.add("user_000006_2");


        assertEquals(expectedSessionIds.size(), sessionIds.size());
        assertEquals(expectedSessionIds, sessionIds);
    }

    @Test
    public void applyRankingBySessionLengthTest() {
        final Dataset<Row> rowDataset = dataProcessor.loadData(path);
        final Dataset<UserLog> userLogDataset = dataProcessor.mapToBean(rowDataset);
        final Dataset<Row> dsWithSessionId = dataProcessor.tagWithSessionId(userLogDataset);
        final Dataset<Row> topRankedSessionDS = dataProcessor.getTopSession(dsWithSessionId,3);

        final Set<String> topRankedSessionIds = topRankedSessionDS.select("sessionId").collectAsList()
                .stream().map(row -> (String) row.getAs(0)).collect(Collectors.toSet());
        final Set<String> expectedTopSessionIds = new HashSet<>();
        expectedTopSessionIds.add("user_000005_1");
        expectedTopSessionIds.add("user_000006_1");
        expectedTopSessionIds.add("user_000006_2");

        assertEquals(expectedTopSessionIds, topRankedSessionIds);
    }

    @Test
    public void getLongestSessionsTest() {
        final Dataset<Row> rowDataset = dataProcessor.loadData(path);
        final Dataset<UserLog> userLogDataset = dataProcessor.mapToBean(rowDataset);
        final Dataset<Row> dataWithSessionIdDS = dataProcessor.tagWithSessionId(userLogDataset);
        final Dataset<Row> topRankedSessionDS = dataProcessor.getTopSession(dataWithSessionIdDS,3);
        final Dataset<Row> rankedDs = dataProcessor.getTopPlayedTracks(topRankedSessionDS,dataWithSessionIdDS,3);

        final Map<String, Long> trackPlayedCountMap = rankedDs.select("trackName", "no_of_times_played")
                .collectAsList()
                .stream()
                .collect(Collectors
                        .toMap(row -> row.getAs("trackName"),
                                row -> row.getAs("no_of_times_played")));

        Map<String,Long> expectedTrackToPlayedCount = new HashMap<>();
        expectedTrackToPlayedCount.put("Parolibre (Live_2009_4_15)", 3L);
        expectedTrackToPlayedCount.put("Glacier (Live_2009_4_15)", 3L);
        expectedTrackToPlayedCount.put("Fuck Me Im Famous (Pacha Ibiza)-09-28-2007", 2L);

        assertEquals(expectedTrackToPlayedCount,trackPlayedCountMap);
    }

}
