.PHONY: verify compose-config up up-streaming up-lake up-warehouse down topics smoke smoke-streaming smoke-lake smoke-warehouse logs

verify:
	mvn -B -ntp clean verify

compose-config:
	docker compose config --quiet

up:
	docker compose up -d --build postgres kafka kafka-init transaction-service outbox-publisher

up-streaming:
	docker compose up -d --build kafka kafka-init flink-jobmanager flink-taskmanager flink-submit

up-lake:
	docker compose up -d --build kafka kafka-init minio minio-init lake-writer

up-warehouse:
	docker compose up -d --build kafka kafka-init warehouse-postgres warehouse-writer

down:
	docker compose down

topics:
	docker compose run --rm kafka-init

smoke:
	./scripts/smoke-test.sh

smoke-streaming:
	./scripts/smoke-test-phase5.sh

smoke-lake:
	./scripts/smoke-test-phase6.sh

smoke-warehouse:
	./scripts/smoke-test-phase7.sh

logs:
	docker compose logs -f transaction-service outbox-publisher kafka flink-jobmanager flink-taskmanager minio lake-writer warehouse-postgres warehouse-writer
