#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A simple Flink job that listens to messages from a Kafka topic.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Kafka, Json, Schema
from pyflink.table.window import Tumble
from pyflink.common.time import Time
import json
import logging


def create_kafka_consumer():
    """
    Creates a Flink job that connects to a Kafka topic and processes messages.
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # For demonstration purposes
    
    # Create table environment
    settings = EnvironmentSettings.new_instance()\
        .in_streaming_mode()\
        .build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # Configure Kafka connection parameters
    kafka_topic = "rail_network"
    kafka_address = "kafka:9093"
    
    # Log connection attempt
    logger.info(f"Attempting to connect to Kafka at {kafka_address}")
    logger.info(f"Will consume messages from topic '{kafka_topic}'")

    # Add required JAR files - adjust paths as necessary
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar;"
                         "file:///opt/flink/lib/kafka-clients-3.6.1.jar;"
                         "file:///opt/flink/lib/flink-json-1.19.0.jar"
    )
    
    # Define the source table schema
    source_ddl = f"""
    CREATE TABLE kafka_source (
        message STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{kafka_topic}',
        'properties.bootstrap.servers' = '{kafka_address}',
        'properties.group.id' = 'rail_network_consumer',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'raw'
    )
    """
    
    # Execute the DDL
    t_env.execute_sql(source_ddl)
    logger.info("Source table DDL executed")
    
    # Check if topic exists and log info about the connection
    try:
        # Query the source table to print messages
        source_table = t_env.from_path("kafka_source")
        
        # Convert to stream for better debugging
        ds = t_env.to_append_stream(
            source_table,
            'message STRING'
        )
        
        def process_message(message):
            """Process and log each incoming message"""
            logger.info(f"Received message: {message}")
            return message
        
        ds.map(process_message).print()
        
        # Execute the job
        logger.info("Starting Flink job to consume messages...")
        env.execute("Kafka Rail Network Consumer")
    except Exception as e:
        logger.error(f"Error during job execution: {e}")


if __name__ == "__main__":
    create_kafka_consumer()