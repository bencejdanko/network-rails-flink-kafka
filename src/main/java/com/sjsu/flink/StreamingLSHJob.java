package com.sjsu.flink; // Use your package name

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingLSHJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingLSHJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink LSH MinHashing Job Setup...");

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
            final String sourceTopic = "rtti-joined"; // Your input topic
            final String sinkTopic = "rtti-lsh-signature";   // Output topic remains the same
            final String consumerGroupId = "flink-lsh-group-java-minhash"; // Changed group id slightly
            final long windowMinutes = 1L; // 1-minute window
            final int numHashFunctions = 128; // *** Configure signature size ***
            final long hashFunctionSeed = 42L; // *** Configure seed for reproducibility ***

            // 2. Define Kafka Source Table DDL (Keep as before)
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `rid` STRING, " +
                "  `uid` STRING, " +
                "  `sch_tpl` STRING, " +
                "  `sch_wta` STRING, " +
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
                "  proctime AS PROCTIME()" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'," +
                "  'json.fail-on-missing-field' = 'false'," +
                "  'json.ignore-parse-errors' = 'true'" +
                ")", sourceTopic, kafkaBootstrapServers, consumerGroupId
            );

            LOG.info("Creating Kafka source table with DDL:\n{}", sourceDDL);
            tEnv.executeSql(sourceDDL);
            LOG.info("Kafka source table '{}' created.", sourceTopic);

            // 3. Define Kafka Sink Table DDL - **Updated column type**
            final String sinkDDL = String.format(
                "CREATE TABLE kafka_sink (" +
                "  rid STRING," +
                "  window_end STRING," +
                // --- CHANGE HERE: Use ARRAY<BIGINT> for the signature ---
                "  minhash_signature ARRAY<BIGINT>," +
                "  PRIMARY KEY (rid, window_end) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" + // Kafka value will be {"rid": ..., "window_end": ..., "minhash_signature": [123, 456, ...]}
                ")", sinkTopic, kafkaBootstrapServers
            );

            LOG.info("Creating Kafka sink table with DDL:\n{}", sinkDDL);
            tEnv.executeSql(sinkDDL);
            LOG.info("Kafka sink table '{}' created.", sinkTopic);

            // 4. Register the LSH Aggregate Function - **Pass parameters**
            tEnv.createTemporarySystemFunction("MINHASH_SIGNATURE",
                new LSHAggregateFunction(numHashFunctions, hashFunctionSeed)); // Pass config here
            LOG.info("MinHash Aggregate Function registered with {} functions and seed {}.", numHashFunctions, hashFunctionSeed);

            // 5. Define the windowed aggregation SQL - **Use new function name and column name**
            final String insertSQL = String.format(
                "INSERT INTO kafka_sink " +
                "SELECT " +
                "  rid, " +
                "  CAST(window_end AS STRING) AS window_end, " +
                // --- CHANGE HERE: Call new function, alias to new column name ---
                "  MINHASH_SIGNATURE(sch_tpl, ts_tpl, event_type) AS minhash_signature " +
                "FROM TABLE(" +
                "  TUMBLE(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '%d' MINUTES)" +
                ") " +
                "WHERE rid IS NOT NULL " +
                "GROUP BY rid, window_start, window_end",
                windowMinutes
             );

            LOG.info("Submitting INSERT INTO statement for MinHashing:\n{}", insertSQL);
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for MinHash signature generation.");

        } catch (Exception e) {
            LOG.error("An error occurred during Flink MinHash job execution:", e);
            throw e;
        }
    }
}