import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import DeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import TypeInformation, Types

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)


class BytesPassthroughDeserializer(DeserializationSchema):
    """
    A DeserializationSchema that reads raw bytes from Kafka
    and passes them through unchanged.
    """
    def __init__(self):
        super().__init__()
        logger.info("Initialized BytesPassthroughDeserializer")

    def deserialize(self, message: bytes) -> bytes:
        return message

    def is_end_of_stream(self, next_element: bytes) -> bool:
        return False

    def get_produced_type(self) -> TypeInformation:
        return Types.BYTES()


def run_kafka_consumer_pipeline():
    try:
        # Create StreamExecutionEnvironment
        env = StreamExecutionEnvironment.get_execution_environment()

        # Kafka Consumer Properties
        topic = "rail_network"
        kafka_address = 'flink-kafka-1:9093'
        kafka_props = {
            'bootstrap.servers': kafka_address,
            'group.id': 'flink-bytes-pipeline-group',
            'auto.offset.reset': 'earliest'
        }
        logger.info(f"Setting up Kafka consumer pipeline for topic '{topic}' at {kafka_address}")

        # Create Kafka Source Connector
        kafka_source = FlinkKafkaConsumer(
            topics=topic,
            # deserialization_schema=BytesPassthroughDeserializer(),
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )

        # Add the source to the environment
        stream = env.add_source(kafka_source)

        stream.map(lambda b: f"Pipelined {len(b)} bytes from Kafka",
                   output_type=Types.STRING()) \
              .print()


        # Execute the Flink job
        logger.info("Starting Kafka Consumer BYTES Pipeline Job...")
        env.execute("Kafka Consumer BYTES Pipeline Job")

    except Exception as e:
        logger.exception(f"Error running the Kafka consumer pipeline job: {e}")

if __name__ == '__main__':
    run_kafka_consumer_pipeline()
