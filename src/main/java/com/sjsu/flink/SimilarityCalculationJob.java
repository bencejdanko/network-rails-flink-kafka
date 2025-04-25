package com.sjsu.flink; // Use your package name

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimilarityCalculationJob {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityCalculationJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink MinHash Similarity Calculation Job Setup...");

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
            final String sourceTopic = "rtti-lsh-signature";   // Read from the signature topic
            final String sinkTopic = "rtti-lsh-similarity"; // Output topic for similarities
            final String consumerGroupId = "flink-lsh-similarity-group"; // New consumer group
            // Optional: Add a similarity threshold if you only want to output pairs above a certain value
            final double similarityThreshold = 0.1; // Example: Only output pairs with >= 10% similarity

            // 2. Define Kafka Source Table DDL (Reading Signatures)
            final String sourceDDL = String.format(
                "CREATE TABLE signature_source (" +
                "  rid STRING," +
                "  window_end STRING," +
                "  minhash_signature ARRAY<BIGINT>," + // Read the signature array
                // Optional: Add watermark if needed for event time joins later,
                // but for processing based on window_end string, it's not strictly required here.
                // "  ts AS TO_TIMESTAMP(window_end, 'yyyy-MM-dd HH:mm:ss.SSS')," +
                // "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                 "  proctime AS PROCTIME() "+ // Processing time might still be useful for Flink internals or other operations
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'latest-offset'," + // Or earliest-offset
                "  'format' = 'json'," +
                "  'json.fail-on-missing-field' = 'false'," +
                "  'json.ignore-parse-errors' = 'true'" +
                ")", sourceTopic, kafkaBootstrapServers, consumerGroupId
            );

            LOG.info("Creating Kafka source table with DDL:\n{}", sourceDDL);
            tEnv.executeSql(sourceDDL);
            LOG.info("Kafka source table '{}' created.", sourceTopic);

            // 3. Define Kafka Sink Table DDL (Writing Similarities)
            final String sinkDDL = String.format(
                "CREATE TABLE similarity_sink (" +
                "  window_end STRING," +
                "  rid1 STRING," +       // ID of the first train in the pair
                "  rid2 STRING," +       // ID of the second train in the pair
                "  similarity DOUBLE" + // The calculated similarity score
                // No primary key needed if just appending results.
                // If using upsert-kafka, define PRIMARY KEY (window_end, rid1, rid2) NOT ENFORCED
                ") WITH (" +
                "  'connector' = 'kafka'," + // Simple Kafka sink
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                // 'key.format' and 'value.format' are not needed for the standard 'kafka' connector
                // unless you want specific key serialization. Default value format is JSON.
                "  'format' = 'json'" + // Output value as JSON
                ")", sinkTopic, kafkaBootstrapServers
            );

            LOG.info("Creating Kafka sink table with DDL:\n{}", sinkDDL);
            tEnv.executeSql(sinkDDL);
            LOG.info("Kafka sink table '{}' created.", sinkTopic);

            // 4. Register the Similarity Calculation UDF
            tEnv.createTemporarySystemFunction("CalculateSimilarity", new CalculateSimilarity());
            LOG.info("Similarity Calculation UDF registered.");

            // 5. Define the SQL for Self-Join and Similarity Calculation
            // This joins the stream with itself based on the window_end identifier.
            final String insertSQL = String.format(
                "INSERT INTO similarity_sink " +
                "SELECT " +
                "  t1.window_end, " +
                "  t1.rid AS rid1, " +
                "  t2.rid AS rid2, " +
                "  CalculateSimilarity(t1.minhash_signature, t2.minhash_signature) AS similarity " +
                "FROM signature_source t1 " +
                "JOIN signature_source t2 ON t1.window_end = t2.window_end " + // Join records from the same original window
                "WHERE t1.rid < t2.rid " + // IMPORTANT: Avoid self-pairs (t1.rid = t2.rid) and duplicate pairs (e.g., (A,B) and (B,A))
                "  AND CalculateSimilarity(t1.minhash_signature, t2.minhash_signature) >= %f", // Optional: Filter by threshold
                similarityThreshold // Substitute the threshold value
             );

            LOG.info("Submitting INSERT INTO statement for similarity calculation:\n{}", insertSQL);
            // Execute the SQL query
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for similarity calculation.");

        } catch (Exception e) {
            LOG.error("An error occurred during Flink Similarity Calculation job execution:", e);
            throw e;
        }
    }
}