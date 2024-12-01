#Start Redpanda cluster
.PHONY: start
start: build
	echo "Starting local Kafka cluster"
	docker compose up -d --remove-orphans

.PHONY: stop
stop:
	echo "Stopping local Kafka cluster"
	docker compose down
