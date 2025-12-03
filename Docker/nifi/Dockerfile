FROM apache/nifi:1.24.0

USER root

# Install additional tools
RUN apt-get update && apt-get install -y curl jq vim && rm -rf /var/lib/apt/lists/*

# Copy custom NiFi properties if needed
# COPY ./tools/nifi.properties /opt/nifi/nifi-current/conf/nifi.properties

# Create templates directory
RUN mkdir -p /opt/nifi/nifi-current/templates

USER nifi

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=5 \
  CMD curl -fk https://localhost:8443/nifi/ || exit 1
