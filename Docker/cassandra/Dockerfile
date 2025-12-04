FROM cassandra:4.1

USER root

# Install additional tools
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER cassandra

CMD ["cassandra", "-f"]
