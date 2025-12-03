FROM debian:bullseye

RUN apt update && apt install -y mariadb-server

COPY ./tools/50-server.cnf /etc/mysql/mariadb.conf.d/.
COPY ./script/start.sh ./start.sh

RUN mkdir /run/mysql

RUN chmod +x ./start.sh

CMD ["./start.sh"]
