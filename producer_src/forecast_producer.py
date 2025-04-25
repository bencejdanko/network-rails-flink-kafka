import os
from dotenv import load_dotenv
load_dotenv()

import stomp
import zlib
import io
import time
import socket
import logging

import xml.etree.ElementTree as ET
import xmltodict
import json

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=f'localhost:{9092}')

logging.basicConfig(format='%(asctime)s %(levelname)s\t%(message)s', level=logging.INFO)

try:
    import pyxb_bindings._ct2
    import PPv16 as pushport_bindings

except ModuleNotFoundError:
    logging.error("Class files not found - please configure the client following steps in README.md!")
    exit(1)

USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
HOSTNAME = os.getenv('HOSTNAME')
HOSTPORT = os.getenv('HOSTPORT')
TOPIC = os.getenv('TOPIC')

KAFKA_TS_TOPIC = 'rtti-ts'

if not USERNAME or not PASSWORD or not HOSTNAME or not HOSTPORT or not TOPIC:
    logging.error("STOMP connection details (USERNAME, PASSWORD, HOSTNAME, HOSTPORT, TOPIC) not fully set in environment variables!")
    exit(1)

CLIENT_ID = socket.getfqdn()
HEARTBEAT_INTERVAL_MS = 15000
RECONNECT_DELAY_SECS = 15

def transform_ts_message(train_status_obj):
    try:
        rid = getattr(train_status_obj, 'rid', None)
        uid = getattr(train_status_obj, 'uid', None)
        ssd = getattr(train_status_obj, 'ssd', None)

        station_updates = []
        if hasattr(train_status_obj, 'Location') and isinstance(train_status_obj.Location, list):
            for location in train_status_obj.Location:
                tpl = getattr(location, 'tpl', None)
                pta = getattr(location, 'pta', None)
                ptd = getattr(location, 'ptd', None)
                et = getattr(location, 'et', None)
                at = getattr(location, 'at', None)

                forecast_time = et if et is not None else at

                station_updates.append({
                    "tpl": tpl,
                    "pta": pta,
                    "ptd": ptd,
                    "forecast_time": forecast_time
                })
        elif hasattr(train_status_obj, 'Location') and not isinstance(train_status_obj.Location, list):
             location = train_status_obj.Location
             tpl = getattr(location, 'tpl', None)
             pta = getattr(location, 'pta', None)
             ptd = getattr(location, 'ptd', None)
             et = getattr(location, 'et', None)
             at = getattr(location, 'at', None)
             forecast_time = et if et is not None else at
             station_updates.append({
                    "tpl": tpl,
                    "pta": pta,
                    "ptd": ptd,
                    "forecast_time": forecast_time
                })

        transformed_data = {
            "rid": rid,
            "uid": uid,
            "ssd": ssd,
            "station_updates": station_updates
        }
        return transformed_data

    except AttributeError as ae:
         logging.error(f"Attribute error during TS message transformation: {ae}. Check pyxb binding structure.")
         return None
    except Exception as e:
        logging.error(f"Error transforming TS message: {e}")
        return None

def connect_and_subscribe(connection):
    if stomp.__version__[0] < '5':
        connection.start()

    connect_header = {'client-id': USERNAME + '-' + CLIENT_ID}
    subscribe_header = {'activemq.subscriptionName': CLIENT_ID}

    logging.info(f"Attempting to connect to {HOSTNAME}:{HOSTPORT} with user {USERNAME}")
    connection.connect(username=USERNAME,
                       passcode=PASSWORD,
                       wait=True,
                       headers=connect_header)

    logging.info(f"Subscribing to topic: {TOPIC}")
    connection.subscribe(destination=TOPIC,
                         id='1',
                         ack='auto',
                         headers=subscribe_header)
    logging.info("Subscription successful.")


class StompClient(stomp.ConnectionListener):
    def on_heartbeat(self):
        logging.info('Received a heartbeat')

    def on_heartbeat_timeout(self):
        logging.error('Heartbeat timeout - connection may be lost.')

    def on_error(self, frame):
        logging.error(f"Received error frame: {frame.body}")

    def on_disconnected(self):
        logging.warning('Disconnected - attempting to reconnect in %s seconds' % RECONNECT_DELAY_SECS)
        time.sleep(RECONNECT_DELAY_SECS)
        exit(-1)

    def on_connecting(self, host_and_port):
        logging.info(f'Attempting to connect to {host_and_port[0]}:{host_and_port[1]}')

    def on_message(self, frame):
        try:
            sequence_number = frame.headers.get('SequenceNumber', 'N/A')
            message_type = frame.headers.get('MessageType', 'N/A')
            logging.info(f'Message sequence={sequence_number}, type={message_type} received')

            msg = zlib.decompress(frame.body, zlib.MAX_WBITS | 32)

            pushport_obj = pushport_bindings.CreateFromDocument(msg)

      
            print("\n--- Parsed Push Port Object Structure ---")
            print(pushport_obj)
            print("-----------------------------------------\n")

            # print(dir(pushport_obj))
            if message_type == 'TS':
                logging.info("Processing TS message...")
                try:
                    train_status_obj = getattr(pushport_obj, 'ts', None)

                    if train_status_obj:
                        transformed_data = transform_ts_message(train_status_obj)
                        print("\n--- Transformed TS Message ---")
                        print(transformed_data)
                        print("------------------------------\n")
                        if transformed_data:
                            json_payload = json.dumps(transformed_data).encode('utf-8')
                            producer.send(KAFKA_TS_TOPIC, json_payload)
                            logging.info(f"Produced TS message for RID {transformed_data.get('rid')} to Kafka topic {KAFKA_TS_TOPIC}")
                        else:
                             logging.warning("TS message transformation failed or returned None.")
                    else:
                         logging.warning("TS message received but TrainStatus object not found on Push Port object.")

                except Exception as process_e:
                    logging.error(f"Error processing TS message from Push Port object: {process_e}")

            elif message_type == 'SF':
                 logging.info("Ignoring SF message type.")
                 pass

            elif message_type == 'AS':
                 logging.info("Ignoring AS message type.")
                 pass

            else:
                logging.info(f"Received unhandled message type: {message_type}")

        except zlib.error as zlib_e:
            logging.error(f"Zlib decompression error: {zlib_e}")
        except Exception as general_e:
            logging.error(f"General error processing message (during Push Port parsing): {general_e}")

if __name__ == "__main__":
    conn = stomp.Connection12([(HOSTNAME, HOSTPORT)],
                              auto_decode=False,
                              heartbeats=(HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS))

    conn.set_listener('', StompClient())

    try:
        connect_and_subscribe(conn)
    except Exception as e:
        logging.error(f"Failed to connect or subscribe: {e}")
        exit(1)

    logging.info("Client started. Listening for messages...")
    while True:
        time.sleep(1)

    logging.info("Client shutting down.")
    conn.disconnect()

# Process and send to Kafka
process_message(xml_schedule, 'schedule')
process_message(xml_forecast, 'forecast')
