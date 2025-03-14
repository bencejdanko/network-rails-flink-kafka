National Rail Open Data Python Example
======================================

This Repo was initialized using the National Rail Open Data Python Example (https://github.com/openraildata/stomp-client-python/tree/main)

How to use 
======================================

Notice the generated xml parsing class files (_ct.py, _ct2.py, _ct3.py, etc). Explore them and see the different attributes they each offer.

In the main client (opendata-nationalrail-client.py), you can import these individual files, and then bind the xml to these classes. See how to do so in the `on_message()` function.


Start kafka enviroment
=====================================

Run the docker compose file:

```bash
cd installations/kafka

docker-compose up -d
```

create a topic:

```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic rail_network --bootstrap-server localhost:9092
```

Run the producer
=====================================

```bash
# ensure you have created a venv and installed requirements.txt
source venv/bin/activate

python3 opendata-nationrail-client.py
```

Check produced messages
=====================================

You can test check if the messages have been produced with:

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rail_network --from-beginning
```