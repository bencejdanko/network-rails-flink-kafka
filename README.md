What is this?
=====================================

This a Flink-Kafka stack setup for analyzing the Network Rails feed. Follow the instructions to start consuming the feed and conducting analysis with the Flink SQL CLI.


Start the dockerized enviroment
=====================================

```bash
cd installations/flink

# First, build the image
docker build -t flink -f flink.Dockerfile .

# then, use the docker compose file to create the enviroment
docker-compose up -d
```

Create the kafka topic
===================================

```bash
docker exec -it flink_kafka_1 kafka-topics.sh --create --topic rail_network  --bootstrap-server flink_kafka_1:9093
```

Alternatively, delete it:

```bash
docker exec -it flink_kafka_1 kafka-topics.sh --delete --topic rail_network  --bootstrap-server flink_kafka_1:9093
```

Start the producer
======================================

```bash
# ensure you have created a venv and installed requirements.txt
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 opendata-nationrail-client.py
```

Check produced messages
=====================================

You can test check if the messages have been produced with:

```bash
docker exec -it flink_kafka_1 kafka-console-consumer.sh --bootstrap-server flink_kafka_1:9093 --topic rail_network --from-beginning
```

This Repo was initialized using the National Rail Open Data Python Example (https://github.com/openraildata/stomp-client-python/tree/main)

How to use 
======================================

Notice the generated xml parsing class files (_ct.py, _ct2.py, _ct3.py, etc). Explore them and see the different attributes they each offer.

In the main client (opendata-nationalrail-client.py), you can import these individual files, and then bind the xml to these classes. See how to do so in the `on_message()` function.

Run the consumer
======================================
```bash
docker exec -it jobmanager flink run --python /opt/flink/usrcode/job.py --parallelism 1
```

