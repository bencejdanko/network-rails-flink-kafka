FROM flink:1.20.1-scala_2.12-java17

# Run as root
USER root

# Install dependencies (keep your existing RUN command)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ubuntu-keyring && \
    apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip sudo fuse3 && \
    rm -rf /var/lib/apt/lists/*

# === Continue with your setup ===
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install dependencies and JuiceFS client
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl fuse ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir /juicefs && cd /juicefs && \
    JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v') && \
    curl -s -L https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz | tar -zx && \
    install juicefs /usr/local/bin && \
    cd / && rm -rf /juicefs

# Download connector JARs (keep your existing RUN command)
RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar

COPY requirements.txt /opt/flink/requirements.txt
WORKDIR /opt/flink
RUN pip install --no-cache-dir -r requirements.txt

# Copy entrypoint script and make it executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Use it as the default entrypoint
ENTRYPOINT ["/entrypoint.sh"]
