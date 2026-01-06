#!/bin/sh
set -eu

# Lanza la inicialización en segundo plano
/opt/script/init-cassandra.sh &

# Arranca Cassandra como proceso principal
exec "$@"
