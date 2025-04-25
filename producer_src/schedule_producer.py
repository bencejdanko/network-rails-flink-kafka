import os
from dotenv import load_dotenv
import stomp
import zlib
import gzip
import io
import time
import socket
import logging
import json
import xml.etree.ElementTree as ET
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

# Configure logging with more detail
logging.basicConfig(
    format='%(asctime)s %(levelname)s\t%(message)s',
    level=logging.DEBUG
)

try:
    import PPv16 as pushport_bindings
    import pyxb_bindings._sch3
except ModuleNotFoundError:
    logging.error("Class files not found - please configure the client following steps in README.md!")
    exit(1)

USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
HOSTNAME = os.getenv('HOSTNAME')
HOSTPORT = os.getenv('HOSTPORT')
STOMP_TOPIC = os.getenv('TOPIC', '/topic/darwin.pushport-v16')
KAFKA_TOPIC = 'rtti-schedule'

producer = KafkaProducer(bootstrap_servers=f'localhost:{9092}')

def transform_schedule_message(schedule_obj):
    try:
        schedule_data = {
            "rid": schedule_obj.rid,
            "tpl": schedule_obj.OR.tpl if hasattr(schedule_obj, 'OR') else None,
            "wta": schedule_obj.OR.wta if hasattr(schedule_obj, 'OR') else None,
            "wtd": schedule_obj.OR.wtd if hasattr(schedule_obj, 'OR') else None,
            "wtp": schedule_obj.OR.wtp if hasattr(schedule_obj, 'OR') else None
        }
        logging.info(f"Transformed schedule data: {json.dumps(schedule_data, indent=2)}")
        return json.dumps(schedule_data).encode('utf-8')
    except Exception as e:
        logging.error(f"Error transforming schedule message: {str(e)}")
        return None

def connect_and_subscribe(connection):
    connection.connect(USERNAME, PASSWORD, wait=True)
    connection.subscribe(
        destination=STOMP_TOPIC,
        id=1,
        ack='auto',
        headers={
            'accept-version': '1.2',
            'binary': 'true',
            'auto_content_length': 'false'
        }
    )

def try_gzip_module(message_bytes):
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(message_bytes)) as f:
            return f.read()
    except Exception as e:
        logging.debug(f"gzip module failed: {e}")
        return None

def decompress_darwin_message(message_bytes):
    try:
        # Log the first 32 bytes in both hex and raw format for better debugging
        logging.debug(f"First 32 bytes (hex): {message_bytes[:32].hex()}")
        logging.debug(f"First 32 bytes (raw): {repr(message_bytes[:32])}")
        
        # Step 1: Check if message is already XML
        if message_bytes.strip().startswith(b'<?xml') or message_bytes.strip().startswith(b'<P'):
            logging.debug("Message is already plain XML, skipping decompression.")
            return message_bytes

        # Step 2: Analyze the message structure
        header = message_bytes[:10]
        compressed_data = message_bytes[10:]
        
        logging.debug(f"Message header (hex): {header.hex()}")
        logging.debug(f"Compressed data length: {len(compressed_data)}")

        # Step 3: Try to identify and handle the custom compression format
        if header.startswith(b'\x1f\xfd'):
            logging.debug("Detected custom compression format")
            
            # Try to reconstruct a valid gzip header
            # The original gzip header is 1f 8b 08 00 00 00 00 00 00 00
            # Our header is 1f fd 08 00 00 00 00 00 00 00
            # The difference is in the second byte (8b vs fd)
            reconstructed_header = b'\x1f\x8b' + header[2:]
            reconstructed_data = reconstructed_header + compressed_data
            
            logging.debug(f"Reconstructed header (hex): {reconstructed_header.hex()}")
            
            # Try decompression with the reconstructed header
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(reconstructed_data)) as f:
                    decompressed = f.read()
                    if decompressed and (decompressed.startswith(b'<?xml') or decompressed.startswith(b'<P')):
                        logging.debug("Successfully decompressed with reconstructed gzip header")
                        return decompressed
            except Exception as e:
                logging.debug(f"Gzip decompression with reconstructed header failed: {e}")

            # Try raw deflate on the compressed data with different window sizes
            for window_bits in [15, -15, zlib.MAX_WBITS, -zlib.MAX_WBITS]:
                try:
                    decompressed = zlib.decompress(compressed_data, window_bits)
                    if decompressed and (decompressed.startswith(b'<?xml') or decompressed.startswith(b'<P')):
                        logging.debug(f"Successfully decompressed with window_bits={window_bits}")
                        return decompressed
                except Exception as e:
                    logging.debug(f"Decompression with window_bits={window_bits} failed: {e}")

            # Try XOR decoding the compressed data
            xor_keys = [0xfd, 0x8b, 0x1f, 0x00]
            for key in xor_keys:
                try:
                    decoded_data = bytes(b ^ key for b in compressed_data)
                    decompressed = zlib.decompress(decoded_data, -zlib.MAX_WBITS)
                    if decompressed and (decompressed.startswith(b'<?xml') or decompressed.startswith(b'<P')):
                        logging.debug(f"Successfully decompressed with XOR key 0x{key:02x}")
                        return decompressed
                except Exception as e:
                    logging.debug(f"XOR with key 0x{key:02x} failed: {e}")

        # Step 4: Try standard decompression methods as a fallback
        methods = [
            lambda: zlib.decompress(message_bytes, -zlib.MAX_WBITS),
            lambda: zlib.decompress(message_bytes, zlib.MAX_WBITS),
            lambda: zlib.decompress(message_bytes, 16+zlib.MAX_WBITS),
            lambda: try_gzip_module(message_bytes),
        ]

        for i, method in enumerate(methods):
            try:
                decompressed = method()
                if decompressed and (decompressed.startswith(b'<?xml') or decompressed.startswith(b'<P')):
                    logging.debug(f"Successfully decompressed with method {i}")
                    return decompressed
            except Exception as e:
                logging.debug(f"Method {i} failed: {str(e)}")
                continue

        logging.error("All decompression methods failed")
        return None
    except Exception as e:
        logging.error(f"Error in custom decompression: {str(e)}")
        return None

class StompClient(stomp.ConnectionListener):
    def on_heartbeat(self):
        logging.debug("Received heartbeat")

    def on_heartbeat_timeout(self):
        logging.error("Heartbeat timeout")

    def on_error(self, frame):
        logging.error(f"Received error: {frame.body}")

    def on_disconnected(self):
        logging.info("Disconnected from server")
        time.sleep(5)
        connect_and_subscribe(conn)

    def on_connecting(self, host_and_port):
        logging.info(f"Connecting to {host_and_port}")

    def on_message(self, frame):
        try:
            logging.debug(f"Message headers: {frame.headers}")

            # Handle the case where frame.body is a string (which contains binary data)
            if isinstance(frame.body, str):
                logging.debug(f"Converting string-encoded binary data to bytes (length: {len(frame.body)})")
                # Convert each character to its byte value
                message_bytes = bytes(ord(c) & 0xFF for c in frame.body)
            else:
                message_bytes = frame.body

            decompressed = decompress_darwin_message(message_bytes)
            if decompressed is None:
                logging.error("Failed to decompress message with any method")
                return

            try:
                message = decompressed.decode('utf-8')
            except UnicodeDecodeError:
                message = decompressed.decode('iso-8859-1')

            logging.debug(f"Raw message (first 200 chars): {message[:200]}")

            try:
                root = ET.fromstring(message)
                logging.debug(f"XML root tag: {root.tag}")

                schedule_obj = pyxb_bindings._sch3.CreateFromDocument(message)

                transformed_message = transform_schedule_message(schedule_obj)
                if transformed_message:
                    producer.send(KAFKA_TOPIC, transformed_message)
                    producer.flush()
                    logging.info(f"Successfully produced message to {KAFKA_TOPIC}")

            except ET.ParseError as e:
                logging.error(f"XML parsing error: {str(e)}")
                logging.error(f"Message content (first 200 chars): {message[:200]}")
            except Exception as e:
                logging.error(f"Error processing XML: {str(e)}")
                logging.error(f"Message content (first 200 chars): {message[:200]}")

        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            if 'message' in locals():
                logging.error(f"Message content (first 200 chars): {message[:200]}")

if __name__ == "__main__":
    conn = stomp.Connection12([(HOSTNAME, int(HOSTPORT))])  # Connection12 with raw=True for binary-safe delivery
    conn.set_listener('', StompClient())
    connect_and_subscribe(conn)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        conn.disconnect()
        producer.close()
