package com.sjsu.flink;

// Flink Imports
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

// Jackson Imports
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;


// Java Imports
import java.time.Duration;
import java.util.Properties;
// import java.util.regex.Matcher; // Only needed if using JSON_PATTERN
// import java.util.regex.Pattern; // Only needed if using JSON_PATTERN


public class JoinStreams {

    // Pattern might be needed if Kafka messages are not pure JSON (unlikely)
    // private static final Pattern JSON_PATTERN = Pattern.compile("\\{.*\\}");

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Configuration ---
        String kafkaBootstrapServers = "kafka:9093";  
        String scheduleTopic = "rtti-schedule";     
        String tsTopic = "rtti-ts";                 
        String outputTopic = "rtti-joined";          
        // --- End Configuration ---

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        properties.setProperty("group.id", "flink-join-group-" + scheduleTopic + "-" + tsTopic);

        // Watermark Strategy using 'processing_ts' from JSON as event time
        WatermarkStrategy<Tuple3<String, Long, String>> watermarkStrategy = WatermarkStrategy
                .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // Allow 5 seconds lateness
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                        return element.f1; // Use pre-extracted timestamp (milliseconds)
                    }
                });


        // --- Schedule Stream ---
        FlinkKafkaConsumer<String> scheduleConsumer = new FlinkKafkaConsumer<>(
                scheduleTopic,
                new SimpleStringSchema(),
                properties);
        scheduleConsumer.setStartFromLatest(); // Or set specific start position if needed

        DataStream<Tuple3<String, Long, String>> scheduleStream = env
                .addSource(scheduleConsumer)
                .name("Schedule Kafka Source (" + scheduleTopic + ")") // Add topic to name
                .flatMap(new ParseJson()) // Parse JSON, extract key and timestamp
                .name("Parse Schedule")
                .assignTimestampsAndWatermarks(watermarkStrategy);


        // --- TS Stream ---
        FlinkKafkaConsumer<String> tsConsumer = new FlinkKafkaConsumer<>(
                tsTopic, 
                new SimpleStringSchema(),
                properties);
        tsConsumer.setStartFromLatest(); // Or set specific start position if needed

        DataStream<Tuple3<String, Long, String>> tsStream = env
                .addSource(tsConsumer)
                .name("TS Kafka Source (" + tsTopic + ")") // Add topic to name
                .flatMap(new ParseJson()) // Parse JSON, extract key and timestamp
                .name("Parse TS")
                .assignTimestampsAndWatermarks(watermarkStrategy);


        // --- Join Streams ---
        DataStream<String> joinedStream = scheduleStream
                .join(tsStream)
                .where(schedule -> schedule.f0) // Join key: rid
                .equalTo(ts -> ts.f0)          // Join key: rid
                .window(TumblingEventTimeWindows.of(Time.seconds(30))) // 30-second tumbling event-time window
                .apply(new JoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String>() {
                    // ObjectMapper for parsing JSON within the join function
                    private transient ObjectMapper objectMapper; // Use transient for checkpointing

                    @Override
                    public String join(Tuple3<String, Long, String> schedule, Tuple3<String, Long, String> ts) throws Exception {
                        if (objectMapper == null) {
                            objectMapper = new ObjectMapper();
                        }
                        try {
                            // Re-parse JSON strings (inefficient but functional)
                            JsonNode scheduleJson = objectMapper.readTree(schedule.f2);
                            JsonNode tsJson = objectMapper.readTree(ts.f2);

                            // Construct the output JSON string
                            return String.format("{\"rid\":\"%s\", \"uid\":\"%s\", \"sch_tpl\":\"%s\", \"sch_wta\":\"%s\", \"sch_wtd\":\"%s\", \"sch_wtp\":\"%s\", \"ts_tpl\":\"%s\", \"ts_pta\":\"%s\", \"ts_ptd\":\"%s\", \"ts_wta\":\"%s\", \"ts_wtd\":\"%s\", \"ts_wtp\":\"%s\", \"event_type\":\"%s\", \"actual_time\":\"%s\"}",
                                    schedule.f0, // rid (from key)
                                    scheduleJson.hasNonNull("uid") ? scheduleJson.get("uid").asText() : "null", // sch_uid
                                    scheduleJson.hasNonNull("tpl") ? scheduleJson.get("tpl").asText() : "null", // sch_tpl
                                    scheduleJson.hasNonNull("wta") ? scheduleJson.get("wta").asText() : "null", // sch_wta
                                    scheduleJson.hasNonNull("wtd") ? scheduleJson.get("wtd").asText() : "null", // sch_wtd
                                    scheduleJson.hasNonNull("wtp") ? scheduleJson.get("wtp").asText() : "null", // sch_wtp
                                    tsJson.hasNonNull("tpl") ? tsJson.get("tpl").asText() : "null",             // ts_tpl
                                    tsJson.hasNonNull("pta") ? tsJson.get("pta").asText() : "null",             // ts_pta
                                    tsJson.hasNonNull("ptd") ? tsJson.get("ptd").asText() : "null",             // ts_ptd
                                    tsJson.hasNonNull("wta") ? tsJson.get("wta").asText() : "null",             // ts_wta (planned times might also be in forecast)
                                    tsJson.hasNonNull("wtd") ? tsJson.get("wtd").asText() : "null",             // ts_wtd
                                    tsJson.hasNonNull("wtp") ? tsJson.get("wtp").asText() : "null",             // ts_wtp
                                    tsJson.hasNonNull("event_type") ? tsJson.get("event_type").asText() : "null", // event_type
                                    tsJson.hasNonNull("actual_time") ? tsJson.get("actual_time").asText() : "null" // actual_time
                            );

                        } catch (JsonProcessingException e) {
                             System.err.println("ERROR: Failed to parse JSON during join for rid=" + schedule.f0 + " | Error: " + e.getMessage());
                             // Return a default value or re-throw, depending on desired behavior
                             return "JOIN_ERROR: Could not parse JSON for rid=" + schedule.f0;
                        }
                    }
                });


        // --- Kafka Sink ---
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                outputTopic,
                new SimpleStringSchema(),
                properties);

        joinedStream.addSink(kafkaProducer).name("Joined Data Kafka Sink (" + outputTopic + ")");

        // Execute the Flink job
        System.out.println("Starting Flink Join Streams Job...");
        env.execute("Join Network Rail Schedule and TS Streams Job"); // Descriptive job name
    }

    /**
     * Parses the input JSON string to extract 'rid' (key) and 'processing_ts' (event time).
     * Outputs a Tuple3 containing (rid, timestamp_in_milliseconds, original_json_string).
     * Skips records if 'rid' or 'processing_ts' is missing/invalid.
     */
    public static class ParseJson implements FlatMapFunction<String, Tuple3<String, Long, String>> {

        // ObjectMapper is thread-safe and can be reused
        private transient ObjectMapper objectMapper; // Use transient for checkpointing

        @Override
        public void flatMap(String jsonString, Collector<Tuple3<String, Long, String>> out) {
             if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            // Assuming input string from Kafka is directly the JSON payload
            if (jsonString == null || jsonString.isEmpty() || !jsonString.trim().startsWith("{")) {
                 // Basic check for potentially non-JSON messages
                 return;
            }

            try {
                JsonNode jsonNode = objectMapper.readTree(jsonString);

                // Extract 'rid' - must be non-null text
                if (!jsonNode.hasNonNull("rid") || !jsonNode.get("rid").isTextual()) {
                    // System.err.println("WARN: Missing, null, or non-text 'rid' in JSON: " + jsonString);
                    return; // Skip
                }
                String rid = jsonNode.get("rid").asText();

                // Extract 'processing_ts' for event time - must be non-null and numeric
                if (!jsonNode.hasNonNull("processing_ts") || !jsonNode.get("processing_ts").isNumber()) {
                     // System.err.println("WARN: Missing, null, or non-numeric 'processing_ts' in JSON: " + jsonString);
                     return; // Skip
                }
                double timestampSeconds = jsonNode.get("processing_ts").asDouble();
                long timestampMillis = (long) (timestampSeconds * 1000); // Convert seconds to milliseconds

                // Output: (key, timestamp, original_message_payload)
                out.collect(new Tuple3<>(rid, timestampMillis, jsonString));

            } catch (JsonProcessingException e) {
                // Silently skip messages that fail JSON parsing, or log if needed
                    System.err.println("ERROR: Failed to parse JSON: " + jsonString + " | Error: " + e.getMessage());
            } catch (Exception e) {
                 // Catch other potential errors during processing
                System.err.println("ERROR: Processing message failed: " + jsonString + " | Error: " + e.getMessage());
            }
        }
    }
}
