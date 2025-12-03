#!/bin/bash

MYSQL_INIT_FILE="/createdb.sql"

chown -R mysql: /var/lib/mysql
chmod 777 /var/lib/mysql

mysql_install_db >/dev/null 2>&1

if [ ! -d "/var/lib/mysql/$MYSQL_DATABASE" ]; then
	rm -f "$MYSQL_INIT_FILE"
	echo "CREATE DATABASE $MYSQL_DATABASE;" >> "$MYSQL_INIT_FILE"
	echo "CREATE USER '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';" >> "$MYSQL_INIT_FILE"
	echo "GRANT ALL PRIVILEGES ON $MYSQL_DATABASE.* TO '$MYSQL_USER'@'%';" >> "$MYSQL_INIT_FILE"
	echo "ALTER USER 'root'@'%' IDENTIFIED BY '$MYSQL_ROOT_PASSWORD';" >> "$MYSQL_INIT_FILE"
	echo "FLUSH PRIVILEGES;" >> "$MYSQL_INIT_FILE"
	echo "Starting database ✅"
	mysqld_safe --init-file=$MYSQL_INIT_FILE >/dev/null 2>&1
else
	echo "Starting database ✅"
	mysqld_safe >/dev/null 2>&1
fi
