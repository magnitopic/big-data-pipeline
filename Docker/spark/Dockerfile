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

# Copy initialization script
COPY ./script/start-spark.sh /opt/spark-apps/start-spark.sh
RUN chmod +x /opt/spark-apps/start-spark.sh

USER spark

CMD ["/opt/spark-apps/start-spark.sh"]
