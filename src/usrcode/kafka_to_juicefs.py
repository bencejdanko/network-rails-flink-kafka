from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream import StreamExecutionEnvironment
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Add Kafka connector JAR
env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar")

# Define Kafka source
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("rail_network") \
    .set_group_id("flink-pipeline-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_starting_offsets(KafkaSource.StartingOffsetsInitializer.earliest()) \
    .build()

# Create datastream
stream = env.from_source(
    source=kafka_source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="Kafka Source"
)

# Print messages for debugging
def log_and_forward(value):
    try:
        logger.info(f"Flink received message: {value[:100]}...")  # Log first 100 chars
        return value
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

# Process stream with error handling
stream = stream.map(log_and_forward, output_type=Types.STRING()) \
    .filter(lambda x: x is not None)

# Define JuiceFS sink
sink = FileSink \
    .for_row_format("/mnt/jfs/kafka_data", SimpleStringSchema()) \
    .with_rolling_policy(
        RollingPolicy.default_rolling_policy()
    ) \
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("data")
        .with_part_suffix(".json")
        .build()
    ) \
    .build()

# Attach sink
stream.sink_to(sink)

# Execute
logger.info("Starting Kafka to JuiceFS job...")
env.execute("Kafka to JuiceFS Job")