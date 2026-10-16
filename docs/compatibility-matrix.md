# Compatibility Matrix

This file is the source of truth for versions used by the project. Do not use `latest` for runtime images or dependencies.

| Component | Target | Status | Evidence/source | Checked |
|---|---|---|---|---|
| JDK | 21 | Planned | Verify locally and in CI | 2026-10-16 |
| Maven | 3.8.7+ | Observed locally | `mvn -version` | 2026-10-16 |
| Spring Boot | TBD | Unselected | Choose with JDK 21 support | — |
| Kafka | TBD | Unselected | Choose broker/client pair | — |
| Flink | TBD | Unselected | Verify Java 21/runtime image support | — |
| PostgreSQL | TBD | Unselected | Choose supported image/driver | — |
| MinIO | TBD | Unselected | Pin image and client version | — |
| Airflow | TBD | Unselected | Choose orchestration image | — |

Before Phase 1, replace every `TBD` with an exact version, official source link, compatibility note and verification result.
