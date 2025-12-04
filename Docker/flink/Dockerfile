FROM flink:1.18-scala_2.12-java11

USER root

# Install additional tools
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    vim \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directories for Flink applications and data
RUN mkdir -p /opt/flink-apps /opt/flink-data /opt/flink-data/checkpoints /opt/flink-data/savepoints /opt/flink-data/uploads

# Set proper permissions for flink user
RUN chown -R flink:flink /opt/flink-apps /opt/flink-data

# Copy initialization script
COPY ./script/start-flink.sh /opt/flink-apps/start-flink.sh
RUN chmod +x /opt/flink-apps/start-flink.sh

# Copy Flink configuration (if any custom configs are needed)
COPY ./tools/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

USER flink

CMD ["/opt/flink-apps/start-flink.sh"]
