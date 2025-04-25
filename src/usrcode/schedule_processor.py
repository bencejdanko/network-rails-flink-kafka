from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessFunction
import json

def process_schedule_messages():
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add the Kafka connector dependency
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka_2.12-1.17.1.jar",
                "file:///opt/flink/lib/kafka-clients-3.4.0.jar")
    
    # Define the Kafka consumer properties
    kafka_props = {
        'bootstrap.servers': 'kafka:9093',
        'group.id': 'schedule-processor-group'
    }
    
    # Create the Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        'rtti-schedule',
        SimpleStringSchema(),
        kafka_props
    )
    
    # Set the consumer to start from the earliest message
    kafka_consumer.set_start_from_earliest()
    
    # Add the source to the environment
    stream = env.add_source(kafka_consumer)
    
    # Process the messages
    processed_stream = stream.map(
        lambda x: json.loads(x),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).process(ProcessScheduleFunction())
    
    # Execute the job
    env.execute("Schedule Processor")

class ProcessScheduleFunction(ProcessFunction):
    def process_element(self, value, ctx):
        try:
            # Extract the schedule information
            rid = value.get('rid')
            tpl = value.get('tpl')
            wta = value.get('wta')
            wtd = value.get('wtd')
            wtp = value.get('wtp')
            
            # Log the processed message
            print(f"Processed schedule: RID={rid}, TPL={tpl}, WTA={wta}, WTD={wtd}, WTP={wtp}")
            
            # You can add additional processing logic here
            # For example, writing to a file, database, or another Kafka topic
            
        except Exception as e:
            print(f"Error processing message: {str(e)}")

if __name__ == "__main__":
    process_schedule_messages() 