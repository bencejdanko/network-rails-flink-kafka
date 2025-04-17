#
# National Rail Open Data client demonstrator
# Copyright (C)2019-2024 OpenTrainTimes Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

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
    import PPv16
    import xml_parsers._ct2
    import xml_parsers._sch3
except ModuleNotFoundError:
    logging.error("Class files not found - please configure the client following steps in README.md!")

USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
HOSTNAME = os.getenv('HOSTNAME')
HOSTPORT = os.getenv('HOSTPORT')
TOPIC = os.getenv('TOPIC')

CLIENT_ID = socket.getfqdn()
HEARTBEAT_INTERVAL_MS = 15000
RECONNECT_DELAY_SECS = 15

if USERNAME == '':
    logging.error("Username not set - please configure your username and password in opendata-nationalrail-client.py!")

def serialize(obj):
    """Recursively converts an object into a dictionary"""
    if isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize(v) for v in obj]
    elif hasattr(obj, "__dict__"):  # Class instance
        return {k: serialize(v) for k, v in vars(obj).items()}
    else:
        return obj  # Primitive value

import json

def transform_data(obj):
    uR = obj.get("Pport", {}).get("uR", {})
    train_info = uR.get("TS", {})
    
    # Ensure locations is always a list
    locations = train_info.get("ns5:Location", [])
    if isinstance(locations, dict):  
        locations = [locations]  # Wrap single dictionary into a list
    
    structured_data = {
        "updateOrigin": uR.get("@updateOrigin"),
        "requestSource": uR.get("@requestSource"),
        "requestID": uR.get("@requestID"),
        "train": {
            "rid": train_info.get("@rid"),
            "uid": train_info.get("@uid"),
            "ssd": train_info.get("@ssd")
        },
        "stationUpdates": []
    }

    for loc in locations:
        structured_data["stationUpdates"].append({
            "stationCode": loc.get("@tpl"),
            "workingTimePlanned": loc.get("@wtp"),
            "estimatedTime": loc.get("ns5:pass", {}).get("@at"),
            "platform": loc.get("ns5:plat", {}).get("#text") if isinstance(loc.get("ns5:plat"), dict) else None,
            "platformConfirmed": loc.get("ns5:plat", {}).get("@conf") == "true" if isinstance(loc.get("ns5:plat"), dict) else None
        })
    
    return structured_data



def connect_and_subscribe(connection):
    if stomp.__version__[0] < '5':
        connection.start()

    connect_header = {'client-id': USERNAME + '-' + CLIENT_ID}
    subscribe_header = {'activemq.subscriptionName': CLIENT_ID}

    connection.connect(username=USERNAME,
                       passcode=PASSWORD,
                       wait=True,
                       headers=connect_header)

    connection.subscribe(destination=TOPIC,
                         id='1',
                         ack='auto',
                         headers=subscribe_header)


class StompClient(stomp.ConnectionListener):

    def on_heartbeat(self):
        logging.info('Received a heartbeat')

    def on_heartbeat_timeout(self):
        logging.error('Heartbeat timeout')

    def on_error(self, message):
        logging.error(message)

    def on_disconnected(self):
        logging.warning('Disconnected - waiting %s seconds before exiting' % RECONNECT_DELAY_SECS)
        time.sleep(RECONNECT_DELAY_SECS)
        exit(-1)

    def on_connecting(self, host_and_port):
        logging.info('Connecting to ' + host_and_port[0])

    def on_message(self, frame):
        try:
            logging.info('Message sequence=%s, type=%s received', frame.headers['SequenceNumber'],
                         frame.headers['MessageType'])
            bio = io.BytesIO()
            bio.write(str.encode('utf-16'))
            bio.seek(0)
            msg = zlib.decompress(frame.body, zlib.MAX_WBITS | 32)
            logging.debug(msg)
            # obj = PPv16.CreateFromDocument(msg)
            obj = xml_parsers._sch3.CreateFromDocument(msg)
            obj = xmltodict.parse(msg)
            # logging.info("Successfully received a Darwin Push Port message from %s")
            # logging.info('Raw XML=%s' % msg)
            serialized_obj = serialize(obj)
            #logging.info('Object=%s' % json.dumps(serialized_obj, indent=4))
            transformed_obj = transform_data(serialized_obj)
            logging.info('Object_transformed=%s' % json.dumps(transformed_obj, indent=4))

            producer.send("rail_network", json.dumps(transformed_obj).encode('utf-8'))



        except Exception as e:
            logging.error(str(e))


conn = stomp.Connection12([(HOSTNAME, HOSTPORT)],
                          auto_decode=False,
                          heartbeats=(HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS))

conn.set_listener('', StompClient())
connect_and_subscribe(conn)

while True:
    time.sleep(1)

conn.disconnect()
