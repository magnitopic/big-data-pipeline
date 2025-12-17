FROM cassandra:4.1

USER root

# Install additional tools
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    vim \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy schema and scripts
COPY script/ /opt/script/
COPY tools/ /opt/tools/
RUN chmod +x /opt/script/*.sh

USER cassandra

ENTRYPOINT ["/opt/script/start.sh"]
