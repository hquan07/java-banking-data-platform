# Security Baseline

Local synthetic data is the only permitted data source for the demo. `.env` is ignored; `.env.example` contains only local defaults and must never contain production credentials.

The Phase 4 Compose profile binds PostgreSQL, Kafka and application ports to `127.0.0.1`. Kafka plaintext and replication factor 1 are permitted only for this single-host local profile. Auto topic creation is disabled and topology changes go through the checked-in manifest.

Any non-local profile must add TLS and authenticated authorization/ACLs before use. Kafka certificates must include the internal DNS name `kafka`; clients must not use container IDs or underscore-based aliases. Production-like deployments also require multi-broker replication, `min.insync.replicas`, secret injection outside Compose source, network policy and encrypted backups.

Application containers run as non-root UID 10001; Flink uses the non-root `flink` image user. PostgreSQL, Kafka and Flink checkpoints use named volumes; operators must not delete them during ordinary restart or diagnosis. The local filesystem checkpoint volume is not a durable multi-host store and must be replaced by authenticated, encrypted object storage before any non-local deployment. Backup, restore and credential rotation procedures remain future work and must be tested before this platform is used beyond disposable local data.
