package com.sjsu.flink; // Adjust package name

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DPCountJob {

    private static final Logger LOG = LoggerFactory.getLogger(DPCountJob.class);

    // --- DP Configuration ---
    private static final double DP_SENSITIVITY = 1.0; // For COUNT DISTINCT
    private static final double DP_EPSILON = 1.0;     // Privacy budget (tune this)

    // --- Application Configuration ---
    // Define the location(s) considered sensitive
    // private static final List<String> SENSITIVE_LOCATIONS = Arrays.asList("PADTLL", "WBRNPKS", "PRTOBJP"); // Example sensitive TIPLOCs
    // Define relevant event types (e.g., passing or stopping)
    private static final List<String> RELEVANT_EVENT_TYPES = Arrays.asList("pass", "arr", "dep");


    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Differential Privacy Count Job Setup...");

        try {
            // 1. Set up Flink environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            LOG.info("Flink Execution Environment and Table Environment created.");

            // --- Kafka Configuration ---
            final String kafkaBootstrapServers = "kafka:9093";
            final String sourceTopic = "rtti-joined";
            final String sinkTopic = "rtti-dp-location-counts"; // New output topic
            final String consumerGroupId = "flink-dp-count-group";
            final long windowMinutes = 1L; // Window for counting (e.g., 10 minutes)

            // 2. Define Kafka Source Table DDL (Needs rid, ts_tpl, event_type)
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `rid` STRING, " +
                "  `ts_tpl` STRING, " +         // Location code
                "  `event_type` STRING, " +     // Event type
                // Include other fields if needed but keep minimal for clarity
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
            LOG.info("Creating Kafka source table DDL:\n{}", sourceDDL);
            tEnv.executeSql(sourceDDL);
            LOG.info("Kafka source table '{}' created.", sourceTopic);

            // 3. Define Kafka Sink Table DDL (Output window, location, noisy count)
            final String sinkDDL = String.format(
                "CREATE TABLE kafka_sink (" +
                "  window_end STRING," +
                "  location STRING," +        // The sensitive location being counted
                "  noisy_count DOUBLE" +     // The DP count (using DOUBLE for precision)
                // Note: Using regular kafka sink, not upsert, as each window/location is distinct
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'format' = 'json'" +      // Output JSON like {"window_end":..., "location":..., "noisy_count":...}
                ")", sinkTopic, kafkaBootstrapServers
            );
            LOG.info("Creating Kafka sink table DDL:\n{}", sinkDDL);
            tEnv.executeSql(sinkDDL);
            LOG.info("Kafka sink table '{}' created.", sinkTopic);

            // 4. Register the Laplace Noise UDF
            tEnv.createTemporarySystemFunction("AddLaplaceNoise",
                new AddLaplaceNoise(DP_SENSITIVITY, DP_EPSILON));
            LOG.info("AddLaplaceNoise UDF registered with sensitivity={} epsilon={}", DP_SENSITIVITY, DP_EPSILON);

            // Helper to create SQL IN clauses safely
            // String sensitiveLocationsSQL = SENSITIVE_LOCATIONS.stream()
            //                                 .map(loc -> "'" + loc.replace("'", "''") + "'") // Escape single quotes
            //                                 .collect(Collectors.joining(", "));
            String relevantEventsSQL = RELEVANT_EVENT_TYPES.stream()
                                            .map(evt -> "'" + evt.replace("'", "''") + "'")
                                            .collect(Collectors.joining(", "));


            // 5. Define the windowed aggregation and DP SQL
            final String insertSQL = String.format(
                "INSERT INTO kafka_sink " +
                "SELECT " +
                "  CAST(window_end AS STRING) AS window_end, " +
                "  ts_tpl AS location, " + // The location we grouped by
                // Apply DP noise to the distinct count of train IDs
                "  AddLaplaceNoise( COUNT(DISTINCT rid) ) AS noisy_count " +
                "FROM TABLE(" +
                "  TUMBLE(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '%d' MINUTES)" +
                ") " +
                // Filter for relevant records BEFORE aggregation
                "WHERE " +
                "  rid IS NOT NULL " +
                // "  AND ts_tpl IN (%s) " +          // Filter for sensitive locations
                "  AND event_type IN (%s) " +      // Filter for relevant events
                "GROUP BY window_start, window_end, ts_tpl", // Group by window AND location
                windowMinutes,
                // sensitiveLocationsSQL, // Inject the list of sensitive locations
                relevantEventsSQL      // Inject the list of relevant events
             );
            LOG.info("Submitting INSERT INTO statement for DP Count aggregation:\n{}", insertSQL);
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for Differentially Private Count generation.");

        } catch (Exception e) {
            LOG.error("An error occurred during Flink DP Count job execution:", e);
            throw e;
        }
    }
}