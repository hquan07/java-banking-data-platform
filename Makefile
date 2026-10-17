.PHONY: verify compose-config up up-streaming down topics smoke smoke-streaming logs

verify:
	mvn -B -ntp clean verify

compose-config:
	docker compose config --quiet

up:
	docker compose up -d --build postgres kafka kafka-init transaction-service outbox-publisher

up-streaming:
	docker compose up -d --build kafka kafka-init flink-jobmanager flink-taskmanager flink-submit

down:
	docker compose down

topics:
	docker compose run --rm kafka-init

smoke:
	./scripts/smoke-test.sh

smoke-streaming:
	./scripts/smoke-test-phase5.sh

logs:
	docker compose logs -f transaction-service outbox-publisher kafka flink-jobmanager flink-taskmanager
