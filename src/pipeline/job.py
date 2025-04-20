from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json


def run_kafka_consumer_job():
    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_props = {
        'bootstrap.servers': 'flink-kafka-1:9093',
        'group.id': 'rail-group'
    }

    kafka_topic = 'rail_network'

    consumer = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # Add Kafka source
    ds = env.add_source(consumer).name("Kafka Source")

    # Print each message 
    ds.map(lambda msg: f"Received from Kafka: {msg}", output_type=Types.STRING()) \
      .print()

    env.execute("Kafka Rail Network Consumer Job")


if __name__ == '__main__':
    run_kafka_consumer_job()
