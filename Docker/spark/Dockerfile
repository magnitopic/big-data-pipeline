FROM apache/spark:3.5.0-python3

USER root

# Install additional tools
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Add Spark Cassandra connector jar (Scala 2.12, Spark 3.5)
RUN curl -fsSL -o /opt/spark/jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar

# Create directories for Spark applications and data
RUN mkdir -p /opt/spark-apps /opt/spark-data

# Copy initialization script to a non-shadowed path
COPY ./script/start-spark.sh /usr/local/bin/start-spark.sh
RUN chmod +x /usr/local/bin/start-spark.sh

USER spark
ENTRYPOINT ["/usr/local/bin/start-spark.sh"]
