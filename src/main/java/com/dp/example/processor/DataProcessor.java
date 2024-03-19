package com.dp.example.processor;

import com.dp.example.model.UserLog;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class DataProcessor {
    private final SparkSession spark;

    public static final String CSV_FIELD_DELIMITER = "\t";

    public static final int SESSION_LENGTH_IN_MIN = 20 * 60;

    public static final String ts_format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public final static WindowSpec sortLogTsByUserId =
            Window.partitionBy("userId").orderBy("loginTs");

    public DataProcessor(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> loadData(String csvPath) {
        return spark.read()
                .option("delimiter", CSV_FIELD_DELIMITER)
                .csv(csvPath);
    }

    /**
     * Apply schema and validate the schema for the data.
     * @param userSessionLogs
     * @return
     */
    public Dataset<UserLog> mapToBean(Dataset<Row> userSessionLogs) {
        final Dataset<Row> localDataset = userSessionLogs.withColumn(
                "loginTs",
                to_timestamp(col("_c1"), ts_format)
        );
        return localDataset.map((MapFunction<Row, UserLog>) row ->
                        new UserLog(row.getString(0),
                                row.getAs("loginTs"),
                                row.getString(2),
                                row.getString(3),
                                row.getString(4),
                                row.getString(5)
                        ),
                Encoders.bean(UserLog.class)
        );
    }

    /**
     * Tag all records with unique session identifier
     * @param userLogDS - Schematise music log data
     * @return
     */
    public Dataset<Row> tagWithSessionId(Dataset<UserLog> userLogDS) {

        final Column lastTrackPlayedTs = lag(col("loginTs"), 1).over(sortLogTsByUserId);

        final Column sessionTag = when( lastTrackPlayedTs.isNull(), 1) // check for 1st session
                                 .when( unix_timestamp(col("loginTs")).minus( unix_timestamp(lastTrackPlayedTs)).gt(SESSION_LENGTH_IN_MIN), 1) // check last played track was before 20 mins
                                .otherwise(lit(0)); // combine all sessions

        return userLogDS.withColumn("session_tag", sessionTag)
                .withColumn("sessionId", concat_ws("_",col("userId") , sum(col("session_tag")).over(sortLogTsByUserId)));
    }

    /**
     * Get top Session by track count played.
     * @param dsWithSessionId - Dataset with session Ids
     * @param limit - No. of top ranked session records.
     * @return
     */
    public Dataset<Row> getTopSession(Dataset<Row> dsWithSessionId,int limit) {

        return dsWithSessionId.groupBy("sessionId")
                .agg(count(col("trackName")).as("total_no_track_played"))
                .orderBy(desc("total_no_track_played"), desc("sessionId"))
                .limit(limit)
                ;
    }

    /**
     * Filter out records by joining with top ranked session and get top ranked track played by no. times the track was played.
     * @param topRankedSessionDS - Top session Ids
     * @param dsWithSessionIds - raw data set with session identifier
     * @param topRankForTracks - Cut of rank for track records.
     * @return
     */
    public Dataset<Row> getTopPlayedTracks(Dataset<Row> topRankedSessionDS,
                                           Dataset<Row> dsWithSessionIds, int topRankForTracks) {

        return topRankedSessionDS.join(dsWithSessionIds, "sessionId" )
                .groupBy("trackName")
                .agg(count("trackName").as("no_of_times_played"))
                .orderBy(desc("no_of_times_played"), desc("trackName"))
                .limit(topRankForTracks);

    }
}
