Kafka, Flink, JuiceFS, S3 (minio) realtime streaming integration with Network Rails public feed
=====================================

Authors
=====================================

- Bence Danko
- Jiyoon Lee
- Nairui Liu

What is this?
=====================================

This a Flink-Kafka stack setup for analyzing the Network Rails feed. Follow the instructions to start consuming the feed and:

- Produce to a Kafka topic
- Ingest via Flink
- Sink raw data to S3 storage (Minio) using JuiceFS

Set env variables beforehand
=====================================

Copy paste the `.env.template` as `.env` and fill out the details.


QUICKSTART
====================================

This script will
- Set up the enviroment
- Automatically build and submit jobs:
  - Kafka To Juice: The raw XML consumed from Network Rail is stored to S3 (Minio, AWS, configurable) via JuiceFS.
- Start the Network Rail producer


For output, see the `rails` bucket at http://localhost:9001 (login `ROOT`, password `ROOTROOT`)

See all running jobs at http://localhost:8082/#/overview


Start the dockerized enviroment
=====================================

You need to ensure that your metadata source is properly formatted. First, start up the Redis and Minio containers:

```bash
docker compose up -d redis minio
```

Proceed to format the `minio` storage:

```bash
./format.sh
```

Proceed to start up the other containers:

```bash
docker compose up -d zookeeper kafka jobmanager taskmanager # --build

# Confirm that juicefs mounting the storage was a success with:
docker logs network-rails.taskmanager
docker logs network-rails.jobmanager
```

Connect to the available UIs
===================================

- **Flink**: http://localhost:8082/#/overview
- **Minio**: http://localhost:9001 (login `ROOT`, password `ROOTROOT`)

Run the jobs
======================================

## Java

There is a helper script, `manage-flink-job`, that will automatically build the Java source code, copy the build to `src/usrcode`, and submit the job to Flink, which can be cancelled with `CTRL-C`. 

Examples:

```bash
./manage-flink-job.sh \
    -m com.sjsu.flink.KafkaToJuice \
    -n "Kafka To Juice Operations on network_rail"
```

---

## Python

`src/usrcode` will automatically be mounted to the docker container. Create new jobs/files to upload there. Below is an example of how to run the existing jobs:

```bash
docker exec -it network-rails.jobmanager flink run --python /opt/flink/usrcode/job.py --parallelism 1
docker exec -it network-rails.jobmanager flink run --python /opt/flink/usrcode/job2.py --parallelism 1
docker exec -it network-rails.jobmanager flink run --python /opt/flink/usrcode/kafka_to_juicefs.py --parallelism 1
# add more jobs, etc...
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
docker exec -it network-rails.kafka kafka-console-consumer.sh --bootstrap-server network-rails.kafka:9093 --topic rail_network --from-beginning
docker exec -it network-rails.kafka kafka-console-consumer.sh --bootstrap-server network-rails.kafka:9093 --topic rtti-schedule --from-beginning

docker exec -it network-rails.kafka kafka-console-consumer.sh --bootstrap-server network-rails.kafka:9093 --topic rtti-ts --from-beginning

```

This Repo was initialized using the National Rail Open Data Python Example (https://github.com/openraildata/stomp-client-python/tree/main)