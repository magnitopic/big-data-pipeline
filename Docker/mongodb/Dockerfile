FROM mongo:7.0

# Copy custom configuration if needed
COPY ./tools/mongod.conf /etc/mongod.conf

# Copy initialization script
COPY ./script/init-mongo.sh /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/init-mongo.sh
