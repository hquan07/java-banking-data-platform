.PHONY: verify compose-config security-check backup-restore-drill up up-streaming up-lake up-warehouse up-batch up-airflow up-analytics up-monitoring daily-batch down topics smoke smoke-streaming smoke-lake smoke-warehouse smoke-batch smoke-analytics smoke-operations logs

verify:
	mvn -B -ntp clean verify

compose-config:
	docker compose config --quiet
	docker compose --profile analytics --profile monitoring config --quiet

security-check:
	./scripts/security-check.sh

backup-restore-drill:
	./scripts/backup-restore-drill.sh

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

up-monitoring:
	docker compose --profile analytics --profile monitoring up -d --build postgres warehouse-postgres kafka kafka-init minio minio-init transaction-service outbox-publisher flink-jobmanager flink-taskmanager flink-submit lake-writer warehouse-writer warehouse-migrate analytics-service kafka-exporter postgres-exporter-oltp postgres-exporter-warehouse prometheus grafana

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

smoke-operations:
	./scripts/smoke-test-phase10.sh

logs:
	docker compose --profile analytics --profile monitoring logs -f transaction-service outbox-publisher kafka flink-jobmanager flink-taskmanager minio lake-writer warehouse-postgres warehouse-writer analytics-service prometheus grafana airflow-api-server airflow-scheduler airflow-dag-processor
