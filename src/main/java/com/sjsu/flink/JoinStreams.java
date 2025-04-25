package com.sjsu.flink;

// Flink Imports
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;


// Java Imports
import java.io.IOException; // For ObjectMapper exceptions
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class JoinStreams {

    // Pattern to find the JSON part of the log message
    private static final Pattern JSON_PATTERN = Pattern.compile("\\{.*\\}");

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parse input arguments
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaBootstrapServers = parameterTool.getRequired("kafka.bootstrap.servers");
        String scheduleTopic = parameterTool.getRequired("schedule.topic");
        String tsTopic = parameterTool.getRequired("ts.topic");
        String outputTopic = parameterTool.getRequired("output.topic");

        // Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        properties.setProperty("group.id", "flink-join-group");

        // --- Configure WatermarkStrategy ---
        WatermarkStrategy<Tuple3<String, Long, String>> watermarkStrategy = WatermarkStrategy
                .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                        return element.f1; // Timestamp in milliseconds
                    }
                });


        // --- Schedule Stream ---
        FlinkKafkaConsumer<String> scheduleConsumer = new FlinkKafkaConsumer<>(
                scheduleTopic, new SimpleStringSchema(), properties);
        scheduleConsumer.setStartFromLatest();

        DataStream<Tuple3<String, Long, String>> scheduleStream = env
                .addSource(scheduleConsumer)
                .name("Schedule Kafka Source")
                .flatMap(new ParseJson()) // Use Jackson inside ParseJson
                .name("Parse Schedule")
                .assignTimestampsAndWatermarks(watermarkStrategy);


        // --- TS Stream ---
        FlinkKafkaConsumer<String> tsConsumer = new FlinkKafkaConsumer<>(
                tsTopic, new SimpleStringSchema(), properties);
        tsConsumer.setStartFromLatest();

        DataStream<Tuple3<String, Long, String>> tsStream = env
                .addSource(tsConsumer)
                .name("TS Kafka Source")
                .flatMap(new ParseJson()) // Use Jackson inside ParseJson
                .name("Parse TS")
                .assignTimestampsAndWatermarks(watermarkStrategy);


        // --- Join Streams ---
        DataStream<String> joinedStream = scheduleStream
                .join(tsStream)
                .where(schedule -> schedule.f0) // Key: rid
                .equalTo(ts -> ts.f0)          // Key: rid  
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))        
                .apply(new JoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String>() {
                    // ObjectMapper for parsing JSON within the join function
                    private transient ObjectMapper objectMapper;

                    @Override
                    public String join(Tuple3<String, Long, String> schedule, Tuple3<String, Long, String> ts) throws Exception {
                        if (objectMapper == null) {
                            objectMapper = new ObjectMapper();
                        }
                        try {
                            JsonNode scheduleJson = objectMapper.readTree(schedule.f2);
                            JsonNode tsJson = objectMapper.readTree(ts.f2);

                            // Example combining specific fields (adjust as needed)
                            return String.format("Joined rid=%s: Schedule(uid=%s, tpl=%s) | TS(tpl=%s, event=%s, actual=%s)",
                                    schedule.f0, // rid
                                    scheduleJson.has("uid") ? scheduleJson.get("uid").asText("N/A") : "N/A",
                                    scheduleJson.has("tpl") ? scheduleJson.get("tpl").asText("N/A") : "N/A",
                                    tsJson.has("tpl") ? tsJson.get("tpl").asText("N/A") : "N/A",
                                    tsJson.has("event_type") ? tsJson.get("event_type").asText("N/A") : "N/A",
                                    tsJson.has("actual_time") ? tsJson.get("actual_time").asText("N/A") : "N/A"
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
                outputTopic, new SimpleStringSchema(), properties);

        joinedStream.addSink(kafkaProducer).name("Joined Data Kafka Sink");

        // Execute the Flink job
        System.out.println("Starting Flink Join Streams Job (using Jackson)...");
        env.execute("Join Streams Job");
    }

    /**
     * Parses a log line to extract the JSON part using regex,
     * then parses the JSON using Jackson to extract 'rid' and 'processing_ts'.
     * Outputs a Tuple3 containing (rid, timestamp_in_milliseconds, original_json_string).
     */
    public static class ParseJson implements FlatMapFunction<String, Tuple3<String, Long, String>> {

        // ObjectMapper is thread-safe and can be reused
        private transient ObjectMapper objectMapper; // Use transient for checkpointing

        @Override
        public void flatMap(String logLine, Collector<Tuple3<String, Long, String>> out) {
            // Initialize ObjectMapper if it's null (first time or after deserialization)
             if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            // Extract JSON string from the log line
            Matcher matcher = JSON_PATTERN.matcher(logLine);
            if (!matcher.find()) {
                 System.err.println("WARN: No JSON found in message: " + logLine);
                 return; // Skip if no JSON object is found
            }
            String jsonString = matcher.group();

            try {
                JsonNode jsonNode = objectMapper.readTree(jsonString);

                // Extract 'rid'
                if (!jsonNode.hasNonNull("rid")) { // Check if field exists and is not null
                    System.err.println("WARN: Missing or null 'rid' in JSON: " + jsonString);
                    return; // Skip if 'rid' is missing or null
                }
                String rid = jsonNode.get("rid").asText(); // Use asText() for robustness

                // Extract 'processing_ts' for event time
                if (!jsonNode.hasNonNull("processing_ts")) {
                     System.err.println("WARN: Missing or null 'processing_ts' for event time in JSON: " + jsonString);
                     return; // Skip if timestamp is missing or null
                }

                // Assuming processing_ts is a number (double or long)
                if (!jsonNode.get("processing_ts").isNumber()) {
                    System.err.println("WARN: 'processing_ts' is not a number in JSON: " + jsonString);
                    return; // Skip if not a number
                }
                double timestampSeconds = jsonNode.get("processing_ts").asDouble();
                long timestampMillis = (long) (timestampSeconds * 1000); // Convert to milliseconds

                // Output: (key, timestamp, original_message)
                out.collect(new Tuple3<>(rid, timestampMillis, jsonString));

            } catch (JsonProcessingException e) {
                // Catch Jackson parsing errors
                System.err.println("ERROR: Failed to parse JSON using Jackson: " + jsonString + " | Error: " + e.getMessage());
            } catch (Exception e) {
                // Catch other potential errors (e.g., ClassCastException if field type is wrong)
                 System.err.println("ERROR: Processing message failed: " + jsonString + " | Error: " + e.getMessage());
            }
        }
    }
}