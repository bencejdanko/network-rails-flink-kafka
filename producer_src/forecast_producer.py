import json
from kafka import KafkaProducer
from forecast_parser import parse_and_extract 

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def send_to_kafka(topic, data):
    #Send the extracted data to Kafka.
    try:
        producer.send(topic, value=json.dumps(data).encode())
        print(f"Data sent to {topic}: {data}")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

def process_message(xml_string, message_type='schedule'):
    #Process the message based on type and send it to the appropriate Kafka topic
    extracted_data = parse_and_extract(xml_string, message_type)
    
    if extracted_data:
        if message_type == 'schedule':
            send_to_kafka('rtti-schedule', extracted_data)
        elif message_type == 'forecast':
            send_to_kafka('rtti-ts', extracted_data)
        else:
            print(f"Unknown message type: {message_type}")
    else:
        print(f"Error: No data extracted for message type {message_type}")

# Example XMLs for testing (replace with actual XML data later)
xml_schedule = """""" 
xml_forecast = """""" 

# Process and send to Kafka
process_message(xml_schedule, 'schedule')
process_message(xml_forecast, 'forecast')
