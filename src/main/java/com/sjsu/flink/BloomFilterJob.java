package com.sjsu.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BloomFilterJob {

    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterJob.class);

    // --- Bloom Filter Configuration ---
    // These need tuning based on expected distinct locations per train per window (n)
    // and desired false positive rate (p).
    // Formulas: m ≈ -n*ln(p) / (ln(2)^2), k ≈ (m/n) * ln(2)
    // Example: If expecting n=50 distinct locations/train/window, desire p=0.01 (1%)
    // m ≈ -50*ln(0.01) / (ln(2)^2) ≈ -50*(-4.605) / 0.48 ≈ 479 --> Use power of 2, e.g., 512
    // k ≈ (512/50) * ln(2) ≈ 10.24 * 0.693 ≈ 7 --> Use k=7
    private static final int BLOOM_FILTER_SIZE = 512; // m (bits)
    private static final int BLOOM_FILTER_HASHES = 7; // k
    private static final long BLOOM_FILTER_SEED = 123L; // Seed for reproducibility

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Bloom Filter Job Setup...");

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
            final String kafkaSinkTopic = "rtti-bloomfilter-results"; // New output topic
            final String fsSinkPath = "/mnt/jfs/bloomfilter_data";
            final String consumerGroupId = "flink-bloomfilter-group";
            final long windowMinutes = 1L; // Aggregation window

            // 2. Define Kafka Source Table DDL (Needs rid, ts_tpl)
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `rid` STRING, " +
                "  `ts_tpl` STRING, " + // The location code we want to add to the filter
                // Include other fields if needed for filtering, but keep it minimal
                "  `event_type` STRING, " + // Example: Maybe only add on 'arr' or 'pass'?
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

            // 3. Define Kafka Sink Table DDL (Output rid, window, filter)
            final String kafkaSinkDDL = String.format(
                "CREATE TABLE kafka_bloom_sink (" +
                "  rid STRING," +
                "  window_end STRING," +
                "  bloom_filter_base64 STRING," + // Store the Base64 encoded filter
                "  PRIMARY KEY (rid, window_end) NOT ENFORCED" + // Upsert for latest filter per train/window
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" +
                ")", kafkaSinkTopic, kafkaBootstrapServers
            );
            LOG.info("Creating Kafka sink table DDL:\n{}", kafkaSinkDDL);
            tEnv.executeSql(kafkaSinkDDL);
            LOG.info("Kafka sink table '{}' created.", kafkaSinkTopic);

            // 4. Define Filesystem Sink Table DDL (Append Sink)
            final String fsSinkDDL = String.format(
                "CREATE TABLE filesystem_bloom_sink (" + 
                "  rid STRING," +          
                "  window_end STRING," +
                "  bloom_filter_base64 STRING" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '%s'," +    
                "  'format' = 'json'," + 
                "  'sink.rolling-policy.file-size' = '128MB'," + 
                "  'sink.rolling-policy.rollover-interval' = '10 min'" + 
                ")", fsSinkPath
            );
            LOG.info("Creating Filesystem sink table DDL:\n{}", fsSinkDDL);
            tEnv.executeSql(fsSinkDDL);
            LOG.info("Filesystem sink table created at path '{}'.", fsSinkPath);


            // 5. Register the Bloom Filter Aggregate Function
            tEnv.createTemporarySystemFunction("BF_AGG",
                new BloomFilterAggregateFunction(BLOOM_FILTER_SIZE, BLOOM_FILTER_HASHES, BLOOM_FILTER_SEED));
            LOG.info("Bloom Filter Aggregate Function registered.");

            /*
            // 6. Define the windowed aggregation SQL
            final String insertSQL = String.format(
                "INSERT INTO kafka_sink " +
                "SELECT " +
                "  rid, " +
                "  CAST(window_end AS STRING) AS window_end, " +
                "  BF_AGG(ts_tpl) AS bloom_filter_base64 " + // Call the UDAF on the location field
                "FROM TABLE(" +
                "  TUMBLE(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '%d' MINUTES)" +
                ") " +
                "WHERE rid IS NOT NULL " + // Ensure we have a train ID
                // Optional: Filter which events add to the filter, e.g.:
                // "  AND event_type IN ('arr', 'dep', 'pass') " +
                "GROUP BY rid, window_start, window_end", // Group by train and window
                windowMinutes
             );
            LOG.info("Submitting INSERT INTO statement for Bloom Filter aggregation:\n{}", insertSQL);
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for Bloom Filter generation.");
            */

            // 6. Define the aggregation logic as a VIEW 
            final String viewDDL = String.format(
                "CREATE TEMPORARY VIEW bloom_filter_results_view AS " +
                "SELECT " +
                "  rid, " +
                "  CAST(window_end AS STRING) AS window_end, " +
                "  BF_AGG(ts_tpl) AS bloom_filter_base64 " + 
                "FROM TABLE(" +
                "  TUMBLE(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '%d' MINUTES)" +
                ") " +
                "WHERE rid IS NOT NULL " + 
                "GROUP BY rid, window_start, window_end", // Group by train and window
                windowMinutes
             );
            LOG.info("Creating TEMPORARY VIEW bloom_filter_results_view with DDL:\n{}", viewDDL);
            tEnv.executeSql(viewDDL);
            LOG.info("Temporary view 'bloom_filter_results_view' created.");

            // 7. Create a StatementSet to execute multiple INSERT statements
            StatementSet statementSet = tEnv.createStatementSet();

            // Define the INSERT INTO statement for the Kafka Sink
            final String insertKafkaSQL =
                "INSERT INTO kafka_bloom_sink " + // Target the Kafka sink table
                "SELECT rid, window_end, bloom_filter_base64 FROM bloom_filter_results_view";

            LOG.info("Adding Kafka INSERT statement to StatementSet:\n{}", insertKafkaSQL);
            statementSet.addInsertSql(insertKafkaSQL);

            // Define the INSERT INTO statement for the Filesystem Sink
            final String insertFsSQL =
                "INSERT INTO filesystem_bloom_sink " + // Target the Filesystem sink table
                "SELECT rid, window_end, bloom_filter_base64 FROM bloom_filter_results_view";

            LOG.info("Adding Filesystem INSERT statement to StatementSet:\n{}", insertFsSQL);
            statementSet.addInsertSql(insertFsSQL);


            // 8. Execute the StatementSet
            LOG.info("Executing StatementSet to start data flow to Kafka and Filesystem sinks...");
            statementSet.execute(); // Submits the job
            LOG.info("Flink job submitted and running.");
            
        } catch (Exception e) {
            LOG.error("An error occurred during Flink Bloom Filter job execution:", e);
            throw e;
        }
    }
}
