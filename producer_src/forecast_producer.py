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
    # Corrected import path for PPv16 binding
    import PPv16 as pushport_bindings
    # Assuming other bindings like _ct2 and _for are also directly accessible or handled by pyxb
    import pyxb_bindings._ct2
    import pyxb_bindings._for

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
    transformed_station_updates = []

    try:
        # --- Temporarily print attributes of the train_status_obj for inspection ---
        # print("\n--- Attributes of Individual Train Status Object ---")
        # print(dir(train_status_obj))
        # print("----------------------------------------------------\n")

        rid = getattr(train_status_obj, 'rid', None)
        uid = getattr(train_status_obj, 'uid', None)

        locations_list = getattr(train_status_obj, 'Location', None)

        if locations_list:
            if not isinstance(locations_list, list):
                locations_list = [locations_list]

            for location in locations_list:
                # --- Temporarily print attributes of the location object for inspection ---
                print("\n--- Attributes of Location Object ---")
                print(dir(location))
                print("-------------------------------------\n")
                # You will need to uncomment this to find the correct attribute names for tpl, pta, ptd, et, and at


                tpl = getattr(location, 'tpl', None)

                pta_obj = getattr(location, 'pta', None)
                pta_str = pta_obj.isoformat() if pta_obj is not None and hasattr(pta_obj, 'isoformat') else None

                ptd_obj = getattr(location, 'ptd', None)
                ptd_str = ptd_obj.isoformat() if ptd_obj is not None and hasattr(ptd_obj, 'isoformat') else None

                et_obj = getattr(location, 'et', None)
                et_str = et_obj.isoformat() if et_obj is not None and hasattr(et_obj, 'isoformat') else None

                at_obj = getattr(location, 'at', None)
                at_str = at_obj.isoformat() if at_obj is not None and hasattr(at_obj, 'isoformat') else None

                forecast_time_str = et_str if et_str is not None else at_str

                transformed_station_updates.append({
                    "rid": rid,
                    "uid": uid,
                    "tpl": tpl,
                    "pta": pta_str,
                    "ptd": ptd_str,
                    "forecast_time": forecast_time_str
                })

        return transformed_station_updates

    except AttributeError as ae:
         logging.error(f"Attribute error during TS message transformation: {ae}. Check pyxb binding structure for individual <TS> element or location object using the dir() output.")
         return []
    except Exception as e:
        logging.error(f"Error transforming TS message: {e}")
        return []

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


            # --- Print the parsed root object and its attributes for inspection ---
            # print("\n--- Parsed Push Port Object Structure ---")
            # print(pushport_obj)
            # print("-----------------------------------------\n")
            # print(dir(pushport_obj)) # Uncomment temporarily to see available attributes

            update_messages_container = getattr(pushport_obj, 'uR', None)


            update_messages_list = []
            if update_messages_container:
                if isinstance(update_messages_container, list):
                    update_messages_list = update_messages_container
                else:
                    update_messages_list = [update_messages_container]

            for message in update_messages_list:
                if hasattr(message, 'TS') and message.TS is not None:
                    logging.info("Found Train Status (TS) data within a message in uR.")
                    try:
                        ts_list_obj = message.TS

                        for train_status_obj in ts_list_obj:
                            transformed_station_data_list = transform_ts_message(train_status_obj)

                            if transformed_station_data_list:
                                # --- Print the transformed JSON data for verification ---
                                # Print each flat station update JSON separately
                                for station_data in transformed_station_data_list:
                                    # --- Temporarily print types of values in the dictionary before JSON serialization ---
                                    # print("--- Types of values in station_data dictionary ---")
                                    # for key, value in station_data.items():
                                    #     if not isinstance(value, (str, int, float, bool, type(None), list, dict)):
                                    #          print(f"Key: {key}, Type: {type(value)}")
                                    # print("--------------------------------------------------\n")

                                    print("\n--- Transformed TS Station Message ---")
                                    print(json.dumps(station_data, indent=2))
                                    print("--------------------------------------\n")

                                    json_payload = json.dumps(station_data).encode('utf-8')
                                    producer.send(KAFKA_TS_TOPIC, json_payload)
                                    logging.info(f"Produced TS station message for RID {station_data.get('rid')} TPL {station_data.get('tpl')} to Kafka topic {KAFKA_TS_TOPIC}")
                            else:
                                 logging.warning(f"TS message transformation completed for a train but no station updates were found or transformation failed.")

                    except Exception as process_e:
                        logging.error(f"Error processing specific TS message: {process_e}")

            if message_type == 'SC':
                 logging.info("Ignoring Schedule (SC) message type.")
                 pass
            elif message_type == 'AS' and not update_messages_list:
                 logging.info("Ignoring top-level Association (AS) message type.")
                 pass
            elif message_type not in ['TS', 'SC', 'AS']:
                logging.info(f"Received unhandled message type: {message_type}")

        except zlib.error as zlib_e:
            logging.error(f"Zlib decompression error: {zlib_e}")
        except Exception as general_e:
            logging.error(f"General error processing message: {general_e}")

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
