package com.sjsu.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingLSHJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingLSHJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink LSH Pattern Hashing Job Setup...");

        try {
            // 1. Set up the Flink execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            LOG.info("Flink Execution Environment and Table Environment created.");

            // --- Configuration ---
            final String kafkaBootstrapServers = "kafka:9093"; // Ensure this is correct
            final String sourceTopic = "rtti-joined";
            final String sinkTopic = "rtti-lsh-results";   // Output topic
            final String consumerGroupId = "flink-lsh-group-java";
            final long windowMinutes = 5L; // 5-minute window

            // 2. Define Kafka Source Table DDL - **Include ALL fields from your sample JSON**
            // Ensure data types match, use STRING for simplicity if unsure
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `rid` STRING, " +
                "  `uid` STRING, " +
                "  `sch_tpl` STRING, " +
                "  `sch_wta` STRING, " + // Assuming times are strings initially
                "  `sch_wtd` STRING, " +
                "  `sch_wtp` STRING, " +
                "  `ts_tpl` STRING, " +
                "  `ts_pta` STRING, " +
                "  `ts_ptd` STRING, " +
                "  `ts_wta` STRING, " +
                "  `ts_wtd` STRING, " +
                "  `ts_wtp` STRING, " +
                "  `event_type` STRING, " +
                "  `actual_time` STRING, " +
                // Add other fields if they exist in your actual data
                "  proctime AS PROCTIME()" + // Processing time for windowing
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'," +
                "  'json.fail-on-missing-field' = 'false'," + // Be flexible
                "  'json.ignore-parse-errors' = 'true'" +     // Tolerate bad JSON messages
                ")", sourceTopic, kafkaBootstrapServers, consumerGroupId
            );

            LOG.info("Creating Kafka source table with DDL:\n{}", sourceDDL);
            tEnv.executeSql(sourceDDL);
            LOG.info("Kafka source table '{}' created.", sourceTopic);

            // 3. Define Kafka Sink Table DDL
            // Output: train run ID, window end time, and the calculated pattern hash
            final String sinkDDL = String.format(
                "CREATE TABLE kafka_sink (" +
                "  rid STRING," +
                "  window_end STRING," +
                "  pattern_hash BIGINT," + // Changed from count_estimate
                "  PRIMARY KEY (rid, window_end) NOT ENFORCED" + // Composite key
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," + // Use upsert for latest hash per window/rid
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" +
                ")", sinkTopic, kafkaBootstrapServers
            );

            LOG.info("Creating Kafka sink table with DDL:\n{}", sinkDDL);
            tEnv.executeSql(sinkDDL);
            LOG.info("Kafka sink table '{}' created.", sinkTopic);

            // 4. Register the LSH Aggregate Function
            tEnv.createTemporarySystemFunction("LSH_HASH", new LSHAggregateFunction());
            LOG.info("LSH Aggregate Function registered.");

            // 5. Define the windowed aggregation SQL
            final String insertSQL = String.format(
                "INSERT INTO kafka_sink " +
                "SELECT " +
                "  rid, " +                                          // Grouping key
                "  CAST(window_end AS STRING) AS window_end, " +     // Window identifier
                "  LSH_HASH(sch_tpl, ts_tpl, event_type) AS pattern_hash " + // Call the UDAF
                "FROM TABLE(" +
                "  TUMBLE(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '%d' MINUTES)" + // Tumbling window
                ") " +
                "WHERE rid IS NOT NULL " + // Basic filter for valid records
                "GROUP BY rid, window_start, window_end", // Group by rid and window
                windowMinutes // Substitute window size into the interval string
             );


            LOG.info("Submitting INSERT INTO statement for LSH hashing:\n{}", insertSQL);
            // This defines the data flow. Flink's Table API planner/executor handles execution.
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for LSH pattern hashing.");

            // No env.execute() needed for Table API SQL jobs when submitting via executeSql insert.

        } catch (Exception e) {
            LOG.error("An error occurred during Flink LSH job execution:", e);
            throw e; // Re-throw to indicate failure
        }
    }
}