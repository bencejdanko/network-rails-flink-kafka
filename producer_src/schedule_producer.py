import os
from dotenv import load_dotenv
import stomp
import zlib
import io
import time
import socket
import logging
import json
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s\t%(message)s', level=logging.INFO)

# Import required modules
try:
    import pyxb_bindings._sch3
except ModuleNotFoundError:
    logging.error("Class files not found - please configure the client following steps in README.md!")

# Environment variables
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
HOSTNAME = os.getenv('HOSTNAME')
HOSTPORT = os.getenv('HOSTPORT')
TOPIC = os.getenv('TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def extract_schedule_fields(schedule_obj):
    """Extract relevant fields from the schedule object"""
    try:
        # Get the schedule message
        schedule = schedule_obj.Pport.uR.TS
        
        # Extract the required fields
        schedule_data = {
            "rid": schedule.rid,
            "tpl": schedule.tpl,
            "wta": schedule.wta if hasattr(schedule, 'wta') else None,
            "wtd": schedule.wtd if hasattr(schedule, 'wtd') else None,
            "wtp": schedule.wtp if hasattr(schedule, 'wtp') else None
        }
        
        # Print the extracted data for verification
        logging.info(f"Extracted schedule data: {json.dumps(schedule_data, indent=2)}")
        
        return schedule_data
    except Exception as e:
        logging.error(f"Error extracting schedule fields: {e}")
        return None

class ScheduleStompClient(stomp.ConnectionListener):
    def on_heartbeat(self):
        logging.info('Received a heartbeat')

    def on_heartbeat_timeout(self):
        logging.error('Heartbeat timeout')

    def on_error(self, message):
        logging.error(message)

    def on_disconnected(self):
        logging.warning('Disconnected - waiting 15 seconds before reconnecting')
        time.sleep(15)
        exit(-1)

    def on_connecting(self, host_and_port):
        logging.info('Connecting to ' + host_and_port[0])

    def on_message(self, frame):
        try:
            # Decompress the message
            msg = zlib.decompress(frame.body, zlib.MAX_WBITS | 32)
            
            # Parse the XML into a schedule object
            schedule_obj = pyxb_bindings._sch3.CreateFromDocument(msg)
            
            # Extract the required fields
            schedule_data = extract_schedule_fields(schedule_obj)
            
            if schedule_data:
                # Produce to Kafka
                producer.send('rtti-schedule', schedule_data)
                producer.flush()
                logging.info("Successfully produced schedule message to Kafka")
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")

def connect_and_subscribe(connection):
    if stomp.__version__[0] < '5':
        connection.start()

    connect_header = {'client-id': USERNAME + '-' + socket.getfqdn()}
    subscribe_header = {'activemq.subscriptionName': socket.getfqdn()}

    connection.connect(username=USERNAME,
                      passcode=PASSWORD,
                      wait=True,
                      headers=connect_header)

    connection.subscribe(destination=TOPIC,
                        id='1',
                        ack='auto',
                        headers=subscribe_header)

def main():
    conn = stomp.Connection12([(HOSTNAME, HOSTPORT)],
                            auto_decode=False,
                            heartbeats=(15000, 15000))

    conn.set_listener('', ScheduleStompClient())
    connect_and_subscribe(conn)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        conn.disconnect()

if __name__ == "__main__":
    main() 