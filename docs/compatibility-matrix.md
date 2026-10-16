# Compatibility Matrix

This file is the source of truth for project versions. Runtime images and dependencies must use the exact versions below rather than floating `latest` tags.

| Component | Pinned version | Status | Compatibility and evidence | Checked |
|---|---:|---|---|---|
| JDK | 21 | Verified locally and in CI config | Local OpenJDK `21.0.12.1`; Maven compiler release and GitHub Actions both target 21 | 2026-10-16 |
| Maven | 3.8.7 | Verified locally | `mvn -version`; Spring Boot 3.5 requires Maven 3.6.3+ | 2026-10-16 |
| Spring Boot | 3.5.16 | Selected | [System requirements](https://docs.spring.io/spring-boot/3.5/system-requirements.html): Java 17 through 25 and Maven 3.6.3+ | 2026-10-16 |
| Kafka | 4.3.1 | Selected; Phase 4 runtime validation pending | [Apache download](https://kafka.apache.org/community/downloads/) and [Java support](https://kafka.apache.org/41/operations/java-version/); Java 21 is supported | 2026-10-16 |
| Flink | 2.3.0 | Selected; Phase 5 runtime validation pending | [Apache downloads](https://flink.apache.org/downloads/); Java 21 support was introduced in Flink 2.x, connector/runtime tests remain a Phase 5 gate | 2026-10-16 |
| PostgreSQL | 17.11 | Selected; migration tests provided | [PostgreSQL supported releases](https://www.postgresql.org/docs/release/); Testcontainers image is pinned to `postgres:17.11-alpine` | 2026-10-16 |
| MinIO Community Server | RELEASE.2025-10-15T17-29-55Z | Provisional local-demo pin | The [upstream repository](https://github.com/minio/minio/releases) is archived; reassess replacement and security posture before Phase 6 | 2026-10-16 |
| Airflow | 3.3.2 / Python 3.12 | Selected; Phase 8 runtime validation pending | [Official installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html); use versioned constraints and image `apache/airflow:3.3.2` | 2026-10-16 |

## Upgrade policy

- Version changes require a dedicated pull request, release notes review and the relevant module tests.
- Do not silently move image tags or Maven dependency versions during a feature change.
- Flink, Kafka, MinIO and Airflow selections are recorded now to remove ambiguity; their runtime claims remain unverified until their implementation phase.
- The archived MinIO upstream is an explicit risk. Phase 6 must either accept it as a local-only portfolio dependency with compensating controls or record an ADR selecting another S3-compatible store.
