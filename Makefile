.PHONY: verify compose-config up up-streaming up-lake up-warehouse up-batch up-airflow up-analytics daily-batch down topics smoke smoke-streaming smoke-lake smoke-warehouse smoke-batch smoke-analytics logs

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

up-batch:
	docker compose up -d --build warehouse-postgres minio minio-init
	docker compose --profile batch build batch-processor

up-airflow:
	docker compose --profile airflow up -d --build warehouse-postgres minio minio-init airflow-api-server airflow-scheduler airflow-dag-processor

up-analytics:
	docker compose --profile analytics up -d --build warehouse-postgres warehouse-migrate analytics-service

daily-batch:
	test -n "$(DATE)"
	docker compose --profile batch run --rm batch-processor --job=daily-transactions --date="$(DATE)"

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

smoke-batch:
	./scripts/smoke-test-phase8.sh

smoke-analytics:
	./scripts/smoke-test-phase9.sh

logs:
	docker compose logs -f transaction-service outbox-publisher kafka flink-jobmanager flink-taskmanager minio lake-writer warehouse-postgres warehouse-writer analytics-service airflow-api-server airflow-scheduler airflow-dag-processor
