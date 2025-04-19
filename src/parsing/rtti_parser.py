import sys
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

try:
    # Import the same set of modules as the producer
    import PPv16
    import pyxb_bindings._for
    import pyxb_bindings._sch3
except ImportError as e:
    print(f"Error importing PyXB bindings: {e}")
    print("Please ensure the bindings are generated and accessible in your PYTHONPATH.")
    sys.exit(1)

# --- Kafka Configuration ---
KAFKA_TOPIC = 'rail_network'
KAFKA_BROKERS = ['localhost:9092']

if __name__ == "__main__":
    print(f"Attempting to connect to Kafka brokers: {KAFKA_BROKERS}")
    print(f"Subscribing to topic: {KAFKA_TOPIC}")

    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='earliest',
            consumer_timeout_ms=-1,
        )

        print("Successfully connected to Kafka. Waiting for messages...")

        for message in consumer:
            print(f"\n--- Received Message ---")
            print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")

            # message.value IS NOW BYTES
            xml_bytes = message.value
            if not xml_bytes:
                print("Received empty message value. Skipping.")
                continue

            print(f"Type of received value: {type(xml_bytes)}") # Should print <class 'bytes'>

            try:
                # Pass the BYTES directly to PyXB
                obj = pyxb_bindings._sch3.CreateFromDocument(xml_bytes)
                print(f"Parsed XML successfully (Object: {obj})")
            except Exception as parse_error:
                print(f"!!! PyXB Parsing failed: {parse_error}")


    except NoBrokersAvailable:
        print(f"Error: Could not connect to Kafka brokers at {KAFKA_BROKERS}.")
    except KeyboardInterrupt:
        print("\nConsumer interrupted by user (Ctrl+C). Shutting down.")
    except Exception as e:
        # Catch other errors (Kafka connection, etc.)
        print(f"\nAn unexpected error occurred outside parsing: {e}")
    finally:
        if consumer:
            print("Closing Kafka consumer...")
            consumer.close()
            print("Consumer closed.")