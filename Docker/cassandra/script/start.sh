#!/bin/sh
set -eu

# Arranca Cassandra usando el entrypoint oficial
/usr/local/bin/docker-entrypoint.sh cassandra -f &
CASS_PID=$!

echo "Waiting for Cassandra on 9042..."

# Espera a que el puerto esté disponible
for i in $(seq 1 30); do
  if nc -z localhost 9042 >/dev/null 2>&1; then
    echo "Cassandra is up"
    break
  fi
  sleep 2
done

# Ejecuta inicialización si existe
if [ -f /opt/script/init-cassandra.sh ]; then
  echo "Running init-cassandra.sh"
  sh /opt/script/init-cassandra.sh || true
fi

# Mantiene Cassandra vivo
wait "$CASS_PID"
