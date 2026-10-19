# Dataset Registry

Register every external or generated dataset before use. Raw datasets remain outside Git and namespaces must not be joined unless a documented mapping exists.

| Dataset ID | Source and URL | License/terms | Acquired | Version | SHA-256 | Rows | Schema/mapping | Namespace | Status |
|---|---|---|---|---|---|---:|---|---|---|
| `synthetic-transaction-generator-v1` | Repository-owned deterministic generator; no external download | Project source license; synthetic data only | generated per run | generator contract v1 | record the input manifest/report checksum per benchmark run | record per run | `contracts/transaction-event-v1.schema.json`; account fixture mapping is run-scoped | `LIVE` for API path or `benchmark` for measured load | approved local source |

External datasets discussed in the implementation roadmap, including PaySim, AMLSim and Credit Card Fraud Detection, are not registered or bundled yet. Their roadmap references do not authorize download, redistribution or combination with the synthetic namespace.

For each future entry, store the following evidence next to the benchmark/run report rather than committing raw records:

- canonical source URL and the exact license or terms snapshot;
- acquisition timestamp, upstream version/revision and whole-file SHA-256;
- row count, byte size, delimiter/encoding and a schema/profile summary;
- mapping code/version, rejected-row count and target contract version;
- namespace (`LIVE`, `HISTORICAL_REPLAY` or `benchmark`) and retention owner;
- proof that customer/account identifiers and fraud labels were not silently combined across datasets.

## Local lifecycle and deletion policy

| Data class | Local retention | Deletion authority and protection |
|---|---:|---|
| Kafka raw and validated topics | 7 days | Broker delete policy from the checked-in topic manifest; replay-critical data must already have a committed lake/warehouse copy before expiry |
| Kafka invalid topic | 14 days | Platform operator after quarantine/reconciliation evidence is retained |
| Kafka fraud, aggregate and DLQ topics | 30 days | Platform/data owner; unresolved DLQ records must not be allowed to expire without an incident decision |
| MinIO Bronze / Silver / Gold | 30 / 90 / 365 days | Bucket lifecycle configured by `minio-init`; object versioning protects replacement, but lifecycle expiry is still deletion and requires a documented retention change |
| Warehouse facts, rejects and audit rows | no automatic local expiry | Data owner; delete only through a reviewed migration/job after a verified backup and scoped reconciliation |
| Flink checkpoints | no automatic local expiry | Streaming operator; remove only after a newer recovery point is proven and no running job references the path |
| Prometheus samples | 15 days by default | Monitoring owner through `PROMETHEUS_RETENTION`; metrics are operational evidence, not the authoritative business record |
| Container/application logs | host Docker policy | Host operator; production must set explicit rotation/export and preserve incident logs before cleanup |

Named volumes are never part of ordinary cleanup. `docker compose down -v`, bucket deletion and broad object/database deletion require an explicit disposable-environment decision; backup artifacts must be stored outside the volume whose loss they protect.
