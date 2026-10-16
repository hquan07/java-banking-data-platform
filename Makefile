.PHONY: verify compose-config up down topics smoke logs

verify:
	mvn -B -ntp clean verify

compose-config:
	docker compose config --quiet

up:
	docker compose up -d --build postgres kafka kafka-init transaction-service outbox-publisher

down:
	docker compose down

topics:
	docker compose run --rm kafka-init

smoke:
	./scripts/smoke-test.sh

logs:
	docker compose logs -f transaction-service outbox-publisher kafka
