FROM apache/nifi:2.5.0

USER root

# Install additional tools
RUN apt-get update && apt-get install -y curl jq vim && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /opt/nifi/nifi-current/templates && \
    mkdir -p /opt/nifi/nifi-current/conf/scripts && \
    mkdir -p /opt/nifi/data

# Copy flow configuration and scripts
COPY ./templates/flow.json /opt/nifi/nifi-current/templates/flow.json
COPY ./script/import-flow.sh /opt/nifi/nifi-current/conf/scripts/import-flow.sh
COPY ./script/start.sh /opt/nifi/nifi-current/conf/scripts/start.sh

# Set permissions
RUN chmod +x /opt/nifi/nifi-current/conf/scripts/*.sh && \
    chown -R nifi:nifi /opt/nifi/nifi-current/templates && \
    chown -R nifi:nifi /opt/nifi/nifi-current/conf/scripts && \
    chown -R nifi:nifi /opt/nifi/data

USER nifi

# Set custom entrypoint
CMD ["/opt/nifi/nifi-current/conf/scripts/start.sh"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=5 \
  CMD curl -fk https://localhost:8443/nifi/ || exit 1
