FROM flink:1.19.0-scala_2.12-java17

# Download required dependencies
RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar && \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

# Download the Flink SQL Client JAR
RUN wget -P /opt/flink/ https://archive.apache.org/dist/flink/flink-1.19.0/bin/sql-client.sh
RUN chmod +x /opt/flink/sql-client.sh
