package com.sjsu.flink; // Use your package name

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
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
            final String kafkaSinkTopic = "rtti-lsh-similarity"; // Output topic for similarities
            final String fsSinkPath = "/mnt/jfs/similarity_data";       // Output path for similarities (Filesystem)
            final String consumerGroupId = "flink-lsh-similarity-group"; // New consumer group
            final double similarityThreshold = 0.1; // Example: Only output pairs with >= 10% similarity

            // 2. Define Kafka Source Table DDL (Reading Signatures)
            final String sourceDDL = String.format(
                "CREATE TABLE signature_source (" +
                "  rid STRING," +
                "  window_end STRING," +
                "  minhash_signature ARRAY<BIGINT>," + // Read the signature array
                 "  proctime AS PROCTIME() "+ // Processing time might still be useful for Flink internals or other operations
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
                ")", kafkaSinkTopic, kafkaBootstrapServers
            );

            LOG.info("Creating Kafka sink table with DDL:\n{}", sinkDDL);
            tEnv.executeSql(sinkDDL);
            LOG.info("Kafka sink table '{}' created.", kafkaSinkTopic);

            // 4. Define Filesystem Sink Table DDL (Writing Similarities to Filesystem)
            final String fsSinkDDL = String.format(
                "CREATE TABLE similarity_filesystem_sink (" + // New sink table
                "  window_end STRING," +   // Schema must match the data being inserted
                "  rid1 STRING," +
                "  rid2 STRING," +
                "  similarity DOUBLE" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '%s'," +    // Directory where files will be written
                "  'format' = 'json'," + // Write each row as a JSON line in a text file
                "  'sink.rolling-policy.file-size' = '128MB'," +
                "  'sink.rolling-policy.rollover-interval' = '10 min'" + 
                ")", fsSinkPath
            );

            LOG.info("Creating Filesystem sink table with DDL:\n{}", fsSinkDDL);
            tEnv.executeSql(fsSinkDDL);
            LOG.info("Filesystem sink table created at path '{}'.", fsSinkPath);


            // 5. Register the Similarity Calculation UDF
            tEnv.createTemporarySystemFunction("CalculateSimilarity", new CalculateSimilarity());
            LOG.info("Similarity Calculation UDF registered.");

            // 6. Define the SQL for Self-Join and Similarity Calculation
            // This joins the stream with itself based on the window_end identifier.
            /* 
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
            */

            // 6. Define the calculation logic as a VIEW
            // This joins the stream with itself based on the window_end identifier.
            final String viewDDL = String.format(
                "CREATE TEMPORARY VIEW similarity_results_view AS " + // Use TEMPORARY VIEW
                "SELECT " +
                "  t1.window_end, " +
                "  t1.rid AS rid1, " +
                "  t2.rid AS rid2, " +
                "  CalculateSimilarity(t1.minhash_signature, t2.minhash_signature) AS similarity " +
                "FROM signature_source t1 " +
                "JOIN signature_source t2 ON t1.window_end = t2.window_end " + // Join records from the same original window
                "WHERE t1.rid < t2.rid " + // IMPORTANT: Avoid self-pairs and duplicate pairs
                "  AND CalculateSimilarity(t1.minhash_signature, t2.minhash_signature) >= %f", // Filter by threshold
                similarityThreshold // Substitute the threshold value
             );

            LOG.info("Creating TEMPORARY VIEW similarity_results_view with DDL:\n{}", viewDDL);
            tEnv.executeSql(viewDDL);
            LOG.info("Temporary view 'similarity_results_view' created.");

        
            // 7. Create a StatementSet to execute multiple INSERT statements
            StatementSet statementSet = tEnv.createStatementSet();

            // Define the INSERT INTO statement for the Kafka Sink
            final String insertKafkaSQL =
                "INSERT INTO similarity_kafka_sink " +
                "SELECT window_end, rid1, rid2, similarity FROM similarity_results_view";

            LOG.info("Adding Kafka INSERT statement to StatementSet:\n{}", insertKafkaSQL);
            statementSet.addInsertSql(insertKafkaSQL);

            // Define the INSERT INTO statement for the Filesystem Sink
            final String insertFsSQL =
                "INSERT INTO similarity_filesystem_sink " +
                "SELECT window_end, rid1, rid2, similarity FROM similarity_results_view";

            LOG.info("Adding Filesystem INSERT statement to StatementSet:\n{}", insertFsSQL);
            statementSet.addInsertSql(insertFsSQL);


            // 8. Execute the StatementSet
            LOG.info("Executing StatementSet to start data flow to Kafka and Filesystem sinks...");
            statementSet.execute();
            // For INSERT statements submitted via StatementSet, execute() blocks until the job finishes
            LOG.info("Flink job submitted and running.");

        } catch (Exception e) {
            LOG.error("An error occurred during Flink Similarity Calculation job execution:", e);
            throw e;
        }
    }
}
