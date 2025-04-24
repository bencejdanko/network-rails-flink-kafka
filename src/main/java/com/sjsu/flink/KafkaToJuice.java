package com.sjsu.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToJuice {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToJuice.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Kafka to Filesystem Job Setup...");

        try {
            // 1. Set up the Flink execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // Use Streaming mode for Table API
            final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            // Create the Table Environment
            final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            LOG.info("Flink Execution Environment and Table Environment created.");

            // --- Configuration ---
            final String kafkaBootstrapServers = "kafka:9093"; // Kafka broker address
            final String sourceTopic = "rail_network";         // Source Kafka topic with raw XML data
            final String sinkPath = "/mnt/jfs/kafka_data";     // Target filesystem path
            final String consumerGroupId = "flink-raw-xml-group-java"; // Kafka consumer group

            // 2. Define Kafka Source Table DDL for raw data
            // Assuming the Kafka messages are UTF-8 encoded XML strings
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `message` STRING" + // Read the entire message payload as a string
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'latest-offset'," + // Or 'earliest-offset'
                "  'format' = 'raw'" + // Use 'raw' format to read bytes as is (Flink interprets as UTF-8 String here)
                ")", sourceTopic, kafkaBootstrapServers, consumerGroupId
            );

            LOG.info("Creating Kafka source table with DDL:\n{}", sourceDDL);
            tEnv.executeSql(sourceDDL);
            LOG.info("Kafka source table '{}' created.", sourceTopic);

            // 3. Define Filesystem Sink Table DDL
            // Note: Ensure the Flink job has write permissions to the sinkPath
            // The 'text' format writes each row as a line.
            final String sinkDDL = String.format(
                "CREATE TABLE filesystem_sink (" +
                "  `message` STRING" + // Schema matches the data being written
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '%s'," +    // Directory where files will be written
                "  'format' = 'json'," + // Write each message string as a line in a text file
                "  'sink.rolling-policy.file-size' = '128MB'," + // Roll files based on size
                "  'sink.rolling-policy.rollover-interval' = '10 min'" + // Or roll based on time
                ")", sinkPath
            );

            LOG.info("Creating Filesystem sink table with DDL:\n{}", sinkDDL);
            tEnv.executeSql(sinkDDL);
            LOG.info("Filesystem sink table created at path '{}'.", sinkPath);

            // 4. Define the simple data transfer SQL
            // Read from Kafka source and insert directly into the filesystem sink
            final String insertSQL =
                "INSERT INTO filesystem_sink " +
                "SELECT " +
                "  `message` " + // Select the raw message content
                "FROM kafka_source";

            LOG.info("Submitting INSERT INTO statement for Kafka to Filesystem transfer:\n{}", insertSQL);
            // This defines the data flow and submits it for execution.
            // For SQL jobs, executeSql handles the job submission implicitly for INSERT statements.
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for execution.");

            // No explicit env.execute() needed for Table API INSERT INTO statements.

        } catch (Exception e) {
            LOG.error("An error occurred during Flink job execution:", e);
            throw e; // Re-throw to indicate failure
        }
    }
}