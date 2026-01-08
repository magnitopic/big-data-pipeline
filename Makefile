# INCLUDES #
include .env

# COLOURS #

GREEN = \033[1;32m
COLOR_OFF = \033[0m

# CONFIG #
DOCKER_COMPOSE = docker-compose.yml

all: build

build:
	@docker compose up -d --build

restart: down
	@echo "$(GREEN)<+> STARTING CONTAINERS <+> $(COLOR_OFF)"
	@docker compose -f $(DOCKER_COMPOSE) up -d

stop:
	@echo "$(GREEN)<+> STOPPING CONTAINERS <+> $(COLOR_OFF)"
	@docker compose -f $(DOCKER_COMPOSE) stop

down: stop
	@echo "$(GREEN)<+> DELETING BUILD <+> $(COLOR_OFF)"
	@docker compose -f $(DOCKER_COMPOSE) down -v

remove_data:
	@echo "$(GREEN)<+> REMOVING DATA <+> $(COLOR_OFF)"
	@rm -rf $(DATA_PATH)
	@rm -rf $(UPLOADS_PATH)
	@rm -rf $(DOWNLOADS_PATH)

destroy: down remove_data
	@echo "$(GREEN)<+> REMOVING ALL IMAGES <+> $(COLOR_OFF)"
	@rm -rf $(BACKEND_NODE_MODULES) $(BACKEND_PACKAGE_LOCK)
	@rm -rf $(FRONTEND_NODE_MODULES) $(FRONTEND_PACKAGE_LOCK)
	@docker system prune -af

re: destroy build
	@echo "$(GREEN)<+> RESETTING CONTAINERS <+> $(COLOR_OFF)"

historic:
	@echo "$(GREEN)Submitting historic ETL job to Spark...$(COLOR_OFF)"
	@docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/pysparkHistoricConnector.py

streaming:
	@echo "$(GREEN)Starting streaming pipeline...$(COLOR_OFF)"
	@docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/spark-apps/flights_streaming.py
	@docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/spark-apps/weather_streaming.py