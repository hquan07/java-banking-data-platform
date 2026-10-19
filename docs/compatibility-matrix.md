# Compatibility Matrix

This file is the source of truth for project versions. Runtime images and dependencies must use the exact versions below rather than floating `latest` tags.

| Component | Pinned version | Status | Compatibility and evidence | Checked |
|---|---:|---|---|---|
| JDK | 21 | Verified locally and in CI config | Local OpenJDK `21.0.12.1`; Maven compiler release and GitHub Actions both target 21 | 2026-10-16 |
| Maven | 3.8.7 | Verified locally | `mvn -version`; Spring Boot 3.5 requires Maven 3.6.3+ | 2026-10-16 |
| Spring Boot | 3.5.16 | Phase 9 runtime verified | [System requirements](https://docs.spring.io/spring-boot/3.5/system-requirements.html): Java 17 through 25 and Maven 3.6.3+; transaction, publisher, lake, warehouse and read-only analytics services compile/test, and the analytics API passed live Compose smoke | 2026-10-19 |
| Kafka broker | 4.3.1 | Phase 3 publisher integration verified | `apache/kafka:4.3.1` Testcontainers test covers ACK, outage retention and recovery; reproducible Compose runtime remains Phase 4 | 2026-10-16 |
| Kafka Java client | 3.9.2 | Phase 3 publisher integration verified | Resolved through Spring Boot 3.5.16/Spring Kafka and tested against broker 4.3.1 with idempotence and `acks=all` | 2026-10-16 |
| Kafka Java client (Flink job) | 4.2.0 | Phase 5 runtime verified | Matches the dependency used to build Kafka connector 5.0.0-2.1; shaded JAR and live smoke logs both report 4.2.0 | 2026-10-17 |
| Flink | 2.1.3 / Java 21 | Phase 5 runtime verified | Image `flink:2.1.3-java21`; [Flink 2.0 introduced Java 21 support](https://nightlies.apache.org/flink/flink-docs-release-2.0/release-notes/flink-2.0/). [Flink 2.3 has no published Kafka connector](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/kafka/), so the job stays on the supported 2.1 line | 2026-10-17 |
| Flink Kafka connector | 5.0.0-2.1 | Phase 5 runtime verified | [Official DataStream connector release for Flink 2.1](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/connectors/datastream/kafka/); packaged in the shaded streaming JAR and exercised by the live smoke test | 2026-10-17 |
| PostgreSQL | 17.11 | OLTP, warehouse, batch and Phase 9 analytics verified | Testcontainers and Compose databases are pinned to `postgres:17.11-alpine`; warehouse migrations through V4, replay/quarantine, daily replacement, audit/rerun and read-only bounded analytics queries pass | 2026-10-19 |
| MinIO Community Server | RELEASE.2025-10-15T17-29-55Z | Phase 6 local runtime verified | Built from the pinned [upstream release source](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z) because public prebuilt pulls were unavailable; bucket/versioning/lifecycle and live object write/read are verified. The repository is archived, so ADR 0001 restricts this to local synthetic demo use | 2026-10-18 |
| MinIO Java SDK | 9.0.3 | Phase 6 runtime verified | Pinned [official SDK release](https://github.com/minio/minio-java/releases/tag/9.0.3); upload/stat/list/get paths exercised against the pinned server | 2026-10-18 |
| Apache Parquet Java | 1.18.1 | Phase 6 runtime verified | Pinned [official release](https://github.com/apache/parquet-java/releases/tag/apache-parquet-1.18.1); Avro writer/reader exercised in unit and live MinIO smoke tests | 2026-10-18 |
| Apache Avro | 1.12.1 | Phase 6 runtime verified | Pinned [official release](https://avro.apache.org/blog/2025/10/16/avro-1.12.1/); logical decimal/timestamp schema used by Silver and Gold Parquet | 2026-10-18 |
| Apache Hadoop libraries | 3.4.2 | Phase 6 runtime verified | Pinned [official release](https://hadoop.apache.org/release/3.4.2.html); only local Parquet IO support is used, not a Hadoop cluster | 2026-10-18 |
| Airflow | 3.3.2 / Python 3.12 | Phase 8 local runtime verified | Custom image follows the [official Docker Compose deployment shape](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html) and extends pinned `apache/airflow:3.3.2-python3.12` with Temurin 21 and the batch JAR; API server, scheduler, DAG processor, DAG parse and two same-date four-task reruns passed | 2026-10-19 |
| Docker Engine | 29.9.0 | Phase 4 verified locally | Compose build, KRaft health check, named volumes and smoke test runtime | 2026-10-17 |
| Docker Compose | 2.39.4 | Phase 4 verified locally | `docker compose config`, one-shot `service_completed_successfully` dependency and local runtime | 2026-10-17 |
| Container build JDK | Eclipse Temurin JRE 21.0.9+10 | Phase 4 selected | Runtime image `eclipse-temurin:21.0.9_10-jre`; application processes use non-root UID 10001 | 2026-10-17 |
| Container build Maven | 3.9.11 / Temurin 21 | Phase 4 selected | Builder image `maven:3.9.11-eclipse-temurin-21`; final runtime does not contain Maven | 2026-10-17 |
| Prometheus | 3.13.4 | Phase 10 selected | Pinned security-patch release; source-controlled scrape/rule configuration is validated with `promtool` and the live monitoring smoke | 2026-10-19 |
| Grafana | 13.2.4 | Phase 10 selected | Pinned release; provisioned Prometheus/PostgreSQL datasources and operations dashboard are checked through the live HTTP API | 2026-10-19 |
| Kafka exporter | 1.10.0 | Phase 10 selected | Pinned `danielqsj/kafka-exporter` image; only `banking.*` topics and groups are exposed to the private monitoring network | 2026-10-19 |
| PostgreSQL exporter | 0.20.1 | Phase 10 selected | Pinned `prometheuscommunity/postgres-exporter` image with separate OLTP and warehouse targets and no host ports | 2026-10-19 |

## Upgrade policy

- Version changes require a dedicated pull request, release notes review and the relevant module tests.
- Do not silently move image tags or Maven dependency versions during a feature change.
- Flink remains on the 2.1 line until an official Kafka connector exists for a later runtime and that upgrade passes the same acceptance suite.
- The archived MinIO upstream is an explicit accepted local-only risk documented in ADR 0001. Reassess a maintained S3-compatible implementation and migration path before any non-local profile.
