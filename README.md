What is this?
=====================================

This a Flink-Kafka stack setup for analyzing the Network Rails feed. Follow the instructions to start consuming the feed and conducting analysis with the Flink SQL CLI.


Set env variables beforehand
=====================================

```bash
export UID=$(id -u)
export GID=$(id -g)
```

These will ensure in development, you can freely edit `./src/pipeline` files/jobs.

Start the dockerized enviroment
=====================================

```bash
docker-compose up -d --build
```

Connect to the WebUI
===================================

http://localhost:8082/#/overview

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
docker exec -it network-rails-flink-kafka_kafka_1 kafka-console-consumer.sh --bootstrap-server network-rails-flink-kafka_kafka_1:9093 --topic rail_network --from-beginning
```

This Repo was initialized using the National Rail Open Data Python Example (https://github.com/openraildata/stomp-client-python/tree/main)

Run the jobs
======================================

`src/usrcode` will automatically be mounted to the docker container. Create new jobs/files to upload there. Below is an example of how to run the existing jobs:

```bash
docker exec -it jobmanager flink run --python /opt/flink/usrcode/job.py --parallelism 1
docker exec -it jobmanager flink run --python /opt/flink/usrcode/job2.py --parallelism 1
# add more jobs, etc...
```

Confirm Flink libraries
=================

```bash
docker exec -it jobmanager bash -c "cd /opt/flink/lib && ls"
```