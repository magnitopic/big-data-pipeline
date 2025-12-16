FROM apache/spark:3.5.0-python3

USER root

# Install additional tools
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directories for Spark applications and data
RUN mkdir -p /opt/spark-apps /opt/spark-data

# Copy initialization script to a non-shadowed path
COPY ./script/start-spark.sh /usr/local/bin/start-spark.sh
RUN chmod +x /usr/local/bin/start-spark.sh

USER spark
ENTRYPOINT ["/opt/spark-apps/start-spark.sh"]
