FROM confluentinc/cp-kafka:7.5.0

USER root

RUN microdnf install -y \
    procps \
    net-tools \
    && microdnf clean all