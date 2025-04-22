#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A simple Flink job that listens to messages from a Kafka topic using Table API.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.types import DataTypes
from pyflink.common.typeinfo import Types
import logging
import os
import json

def create_kafka_consumer():
    """
    Creates a Flink job that connects to a Kafka topic using the Table API.
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # For demonstration purposes
    
    # Create table environment
    table_env = StreamTableEnvironment.create(env)
    
    # Configure Kafka connection parameters
    kafka_topic = "rail_network"
    kafka_address = "kafka:9093"
    
    # Log connection attempt
    logger.info(f"Attempting to connect to Kafka at {kafka_address}")
    logger.info(f"Will consume messages from topic '{kafka_topic}'")

    # First check if JAR files exist (helps with debugging)
    flink_dir = "/opt/flink/lib"
    jar_files = os.listdir(flink_dir) if os.path.exists(flink_dir) else []
    logger.info(f"Available JAR files in {flink_dir}: {jar_files}")
    
    # Find the Kafka connector JAR
    kafka_connector_jar = None
    for jar in jar_files:
        if jar.startswith("flink-sql-connector-kafka") and jar.endswith(".jar"):
            kafka_connector_jar = os.path.join(flink_dir, jar)
            logger.info(f"Found Kafka SQL connector JAR: {kafka_connector_jar}")
            break
    
    if not kafka_connector_jar:
        logger.error("Could not find any Kafka connector JAR!")
        return
    
    # Add the Kafka connector JAR to the classpath - corrected method for Flink 2.0.0
    table_env.get_config().set("pipeline.jars", f"file://{kafka_connector_jar}")
    
    try:
        # Define the Kafka source table using SQL
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
        
        # Execute the DDL statement
        logger.info("Creating Kafka source table")
        table_env.execute_sql(source_ddl)
        
        # Query the table and print results
        logger.info("Querying Kafka source table")
        result_table = table_env.sql_query("SELECT message FROM kafka_source")
        
        # Method 1: Use direct table printing (simplest approach)
        logger.info("Printing table results directly")
        print_result = table_env.execute_sql("SELECT message FROM kafka_source")
        logger.info("Executing print job")
        print_result.print()
        
        # Method 2: Alternative approach if you need to process the data further
        # logger.info("Processing with Table API")
        # result_table.execute().print()
        
    except Exception as e:
        logger.error(f"Error during job execution: {e}")
        # Print more detailed error information
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    create_kafka_consumer()