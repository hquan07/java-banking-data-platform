# Security Baseline

Local synthetic data is the only permitted data source for the demo. `.env` is ignored; `.env.example` contains only local defaults and must never contain production credentials.

The Compose profile binds PostgreSQL, Kafka, MinIO and application/management ports to `127.0.0.1`. Kafka plaintext and replication factor 1 are permitted only for this single-host local profile. Auto topic creation is disabled and topology changes go through the checked-in manifest.

Any non-local profile must add TLS and authenticated authorization/ACLs before use. Kafka certificates must include the internal DNS name `kafka`; clients must not use container IDs or underscore-based aliases. Production-like deployments also require multi-broker replication, `min.insync.replicas`, secret injection outside Compose source, network policy and encrypted backups.

Application containers run as non-root UID 10001; MinIO runs as non-root UID 10002 and Flink uses the non-root `flink` image user. PostgreSQL, Kafka, MinIO and Flink checkpoints use named volumes; operators must not delete them during ordinary restart or diagnosis.

Phase 7 uses a physically separate PostgreSQL service, database, credential set and named volume for analytical data. Its host port and the writer management endpoint remain loopback-only. The local password in `.env.example` is a disposable default, not a production secret. Non-local deployments must provision least-privilege migration/runtime roles, TLS, secret rotation, encrypted backup and restore tests before ingesting anything other than synthetic data. Quarantine stores rejected raw payload bytes, so its access and retention must be at least as strict as the source topic.

Phase 6 creates a private, versioned `banking-lake` bucket. The lake writer uses a dedicated bucket-scoped policy and never receives the MinIO root credential; root credentials are confined to the one-shot bootstrap and local operator commands. Bronze/Silver/Gold lifecycle defaults are 30/90/365 days. These retention rules also apply to synthetic audit data, so change them only with an explicit data-retention decision.

The local MinIO endpoint uses HTTP and the named volume is not encrypted at rest. This is acceptable only for disposable synthetic data on loopback. Any non-local profile must add TLS, external secret injection and rotation, server-side encryption backed by an evaluated KMS, audit logging, backup/restore testing and network policy. The pinned MinIO Community release is built from an archived upstream repository; ADR 0001 records why it is restricted to local demo use and requires a replacement/security review before expansion.

The local filesystem checkpoint volume is not a durable multi-host store and must be replaced by authenticated, encrypted object storage before any non-local deployment. Backup, restore and credential rotation procedures remain future work and must be tested before this platform is used beyond disposable local data.
