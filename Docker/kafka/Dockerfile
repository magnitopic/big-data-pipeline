FROM confluentinc/cp-kafka:7.5.0

USER root

# Install additional tools
RUN microdnf install -y \
    procps \
    net-tools \
    && microdnf clean all

# Create topics directory
RUN mkdir -p /etc/kafka/topics

# Copy initialization script
COPY ./script/init-kafka.sh /etc/kafka/init-kafka.sh
RUN chmod +x /etc/kafka/init-kafka.sh

USER appuser

CMD ["/etc/kafka/init-kafka.sh"]
