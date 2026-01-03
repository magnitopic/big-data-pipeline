FROM cassandra:4.1

USER root

# Herramientas necesarias
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    vim \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia scripts y esquemas
COPY script/ /opt/script/
COPY tools/ /opt/tools/

# Permisos correctos
RUN chmod +x /opt/script/*.sh && \
    chown -R cassandra:cassandra /opt/script /opt/tools

USER cassandra

ENTRYPOINT ["/opt/script/start.sh"]
