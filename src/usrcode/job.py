import logging
import json
import zlib
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

# Set up logging with more detailed level
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

class XMLDeserializer(MapFunction):
    def map(self, value):
        try:
            if value is None:
                return "Received null message"
                
            # Try to process the message
            try:
                # First check if it's compressed (might be a compressed XML string)
                if isinstance(value, str) and value.startswith('\x1f\x8b'):  # gzip magic bytes
                    try:
                        # Convert string to bytes for decompression
                        value_bytes = value.encode('latin-1')
                        decompressed = zlib.decompress(value_bytes, zlib.MAX_WBITS | 32)
                        decoded = decompressed.decode('utf-8', errors='ignore')
                        return f"Decoded (decompressed): {decoded[:200]}..." if len(decoded) > 200 else decoded
                    except Exception as decompress_err:
                        logger.warning(f"Failed to decompress what looked like compressed data: {str(decompress_err)}")
                
                # If it's already a string and not compressed, just return it
                return f"Received message: {value[:200]}..." if len(value) > 200 else value
                
            except Exception as processing_err:
                return f"Error processing message content: {str(processing_err)}"
        except Exception as e:
            return f"Error in deserializer: {str(e)}"

def run_kafka_consumer_pipeline():
    try:
        # Create StreamExecutionEnvironment
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # Enable checkpointing for better Kafka offset management
        env.enable_checkpointing(5000)  # checkpoint every 5 seconds
        
        # Kafka Consumer Properties
        topic = "rail_network"
        kafka_address = 'kafka:9093'
        kafka_props = {
            'bootstrap.servers': kafka_address,
            'group.id': 'flink-pipeline-group',
            'auto.offset.reset': 'earliest',  # Explicitly set to read from the beginning
            'enable.auto.commit': 'false',    # Let Flink handle commits with checkpoints
            'socket.timeout.ms': '10000',     # Increase timeout for debugging
            'session.timeout.ms': '30000',    # Increase session timeout
            'request.timeout.ms': '30000',    # Increase request timeout
            'metadata.max.age.ms': '5000',    # Refresh metadata more frequently
            'max.poll.records': '500'         # Poll more records at once
        }
        logger.info(f"Setting up Kafka consumer pipeline for topic '{topic}' at {kafka_address}")
        logger.info(f"Using consumer properties: {json.dumps(kafka_props, indent=2)}")

        # Create Kafka Source Connector with SimpleStringSchema
        kafka_source = FlinkKafkaConsumer(
            topics=topic,
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )
        
        # Explicitly set to start from the earliest offset to read existing messages
        kafka_source.set_start_from_earliest()
        
        logger.info("Kafka source configured to read from earliest offset with SimpleStringSchema")

        # Add the source to the environment
        stream = env.add_source(kafka_source)
        
        # Process the messages with custom deserializer
        processed_stream = stream.map(
            XMLDeserializer(),
            output_type=Types.STRING()
        )
        
        # Log the processed messages
        processed_stream.map(
            lambda s: (
                logger.info(f"Processed message: {s[:100]}..." if len(s) > 100 else s),
                s
            )[1],
            output_type=Types.STRING()
        ).print()

        # Execute the Flink job
        logger.info("Starting Kafka Consumer Pipeline Job...")
        env.execute("Kafka Consumer Pipeline Job")

    except Exception as e:
        logger.exception(f"Error running the Kafka consumer pipeline job: {e}")
        # Try to provide more specific error information for Kafka issues
        if "Connection refused" in str(e):
            logger.error(f"Kafka connection refused. Check if Kafka broker at {kafka_address} is running")
        elif "Topic rail_network not present" in str(e):
            logger.error(f"Topic '{topic}' does not exist. Check if the topic is created correctly")
        elif "Broker not available" in str(e):
            logger.error(f"Kafka broker not available. Check Kafka cluster health")

if __name__ == '__main__':
    run_kafka_consumer_pipeline()