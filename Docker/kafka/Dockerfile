FROM confluentinc/cp-kafka:7.5.0

USER root

# Herramientas útiles
RUN microdnf install -y \
    procps \
    net-tools \
    && microdnf clean all

# Copiar script de inicialización
COPY ./script/init-kafka.sh /etc/kafka/init-kafka.sh

# Permisos correctos
RUN chmod +x /etc/kafka/init-kafka.sh

# Ejecutar como root (evita errores de permisos)
CMD ["/bin/bash", "/etc/kafka/init-kafka.sh"]
