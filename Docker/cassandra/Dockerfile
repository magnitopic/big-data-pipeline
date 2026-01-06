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
RUN sed -i 's/\r$//' /opt/script/*.sh && \
    chmod +x /opt/script/*.sh && \
    chown -R cassandra:cassandra /opt/script /opt/tools

USER cassandra

SHELL ["/bin/bash", "-c"]

# Use absolute bash path to ensure bash is used (avoids /bin/sh running script)
ENTRYPOINT ["/bin/bash", "-c", "exec /opt/script/start.sh"]
