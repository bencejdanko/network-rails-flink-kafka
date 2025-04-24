from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.common.serialization import SimpleStringEncoder
from pyflink.datastream.connectors import DefaultRollingPolicy
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
import json
import os

def create_kafka_source(env):
    # Define the Kafka consumer properties
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'kafka-to-juicefs-group'
    }
    
    # Create the deserialization schema
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW([Types.STRING()])) \
        .build()
    
    # Create the Kafka source
    return env.add_source(
        FlinkKafkaConsumer(
            topics='rail_network',
            deserialization_schema=deserialization_schema,
            properties=kafka_props
        )
    )

def create_juicefs_sink():
    # Configure the output file settings
    output_config = OutputFileConfig.builder() \
        .with_part_prefix("data") \
        .with_part_suffix(".json") \
        .build()
    
    # Create the file sink that writes to JuiceFS
    return FileSink.for_row_format(
        base_path="/mnt/jfs/kafka_data",
        encoder=SimpleStringEncoder()
    ).with_output_file_config(output_config) \
     .with_rolling_policy(
        # Roll files every 5 minutes or when they reach 128MB
        DefaultRollingPolicy.builder()
            .with_rollover_interval(5 * 60 * 1000)  # 5 minutes
            .with_max_part_size(128 * 1024 * 1024)  # 128MB
            .build()
    ).build()

def main():
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add the Kafka connector JAR
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar")
    
    # Create the Kafka source
    kafka_source = create_kafka_source(env)
    
    # Create the JuiceFS sink
    juicefs_sink = create_juicefs_sink()
    
    # Process the stream and write to JuiceFS
# Process the stream with logging and write to JuiceFS
    kafka_source.map(lambda x: print("Flink received:", x)).sink_to(juicefs_sink)
    
    # Execute the job
    env.execute("Kafka to JuiceFS Streaming Job")

if __name__ == '__main__':
    main() 