FROM cassandra:4.1

USER root

RUN apt-get update && apt-get install -y \
    bash \
    curl \
    procps \
    vim \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY script/ /opt/script/
COPY tools/ /opt/tools/

RUN chmod +x /opt/script/*.sh && \
    chown -R cassandra:cassandra /opt/script /opt/tools

USER cassandra

ENTRYPOINT ["/opt/script/start.sh"]
CMD ["cassandra", "-f"]
