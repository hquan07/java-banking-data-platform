#!/usr/bin/env bash
set -euo pipefail

readonly RUN_TOKEN="$(date +%s)-$$"
readonly RESTORE_DATABASE="phase10_restore_${RUN_TOKEN//-/}"
readonly OBJECT_KEY="operations/restore-drill/run-${RUN_TOKEN}.txt"
readonly DRILL_DIRECTORY="$(mktemp -d /tmp/banking-phase10-restore.XXXXXX)"
readonly WAREHOUSE_BACKUP="${DRILL_DIRECTORY}/warehouse.dump"
readonly OBJECT_BACKUP="${DRILL_DIRECTORY}/minio-object.txt"
WAREHOUSE_WRITER_WAS_RUNNING=false

warehouse_exec() {
  docker compose exec -T warehouse-postgres "$@"
}

minio_shell() {
  docker compose exec -T minio sh -c \
    'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && exec "$@"' \
    sh "$@"
}

cleanup() {
  warehouse_exec dropdb --if-exists --force -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
    "$RESTORE_DATABASE" >/dev/null 2>&1 || true
  minio_shell mc rm --force "local/${MINIO_BUCKET:-banking-lake}/${OBJECT_KEY}" >/dev/null 2>&1 || true
  rm -rf -- "$DRILL_DIRECTORY"
  if [[ "$WAREHOUSE_WRITER_WAS_RUNNING" == "true" ]]; then
    docker compose up -d --no-deps warehouse-writer >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker compose up -d warehouse-postgres minio minio-init

if docker compose ps --status running --services | grep -Fx warehouse-writer >/dev/null; then
  WAREHOUSE_WRITER_WAS_RUNNING=true
  docker compose stop warehouse-writer >/dev/null
fi

warehouse_exec pg_dump \
  -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
  -d "${POSTGRES_WAREHOUSE_DB:-banking_warehouse}" \
  --format=custom --no-owner --no-privileges >"$WAREHOUSE_BACKUP"

warehouse_exec pg_restore --list <"$WAREHOUSE_BACKUP" >/dev/null
warehouse_exec createdb -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" "$RESTORE_DATABASE"
warehouse_exec pg_restore \
  -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
  -d "$RESTORE_DATABASE" \
  --no-owner --no-privileges --exit-on-error <"$WAREHOUSE_BACKUP"

fingerprint_sql="
SELECT CONCAT_WS('|',
  (SELECT COUNT(*) FROM fact_transactions),
  (SELECT COALESCE(SUM(amount), 0) FROM fact_transactions),
  (SELECT COUNT(*) FROM fact_fraud_alerts),
  (SELECT COUNT(*) FROM agg_daily_transactions),
  (SELECT COUNT(*) FROM batch_runs),
  (SELECT COALESCE(MAX(version), '0') FROM flyway_schema_history WHERE success)
);"

SOURCE_FINGERPRINT="$(warehouse_exec psql -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
  -d "${POSTGRES_WAREHOUSE_DB:-banking_warehouse}" -tAc "$fingerprint_sql" | tr -d '[:space:]')"
RESTORED_FINGERPRINT="$(warehouse_exec psql -U "${WAREHOUSE_DB_USERNAME:-banking_warehouse}" \
  -d "$RESTORE_DATABASE" -tAc "$fingerprint_sql" | tr -d '[:space:]')"
[[ "$SOURCE_FINGERPRINT" == "$RESTORED_FINGERPRINT" ]]

printf 'phase10-minio-restore:%s\n' "$RUN_TOKEN" >"$OBJECT_BACKUP"
EXPECTED_OBJECT_SHA="$(sha256sum "$OBJECT_BACKUP" | awk '{print $1}')"
minio_shell mc pipe "local/${MINIO_BUCKET:-banking-lake}/${OBJECT_KEY}" <"$OBJECT_BACKUP" >/dev/null
minio_shell mc cat "local/${MINIO_BUCKET:-banking-lake}/${OBJECT_KEY}" >"${OBJECT_BACKUP}.downloaded"
[[ "$(sha256sum "${OBJECT_BACKUP}.downloaded" | awk '{print $1}')" == "$EXPECTED_OBJECT_SHA" ]]

minio_shell mc rm --force "local/${MINIO_BUCKET:-banking-lake}/${OBJECT_KEY}" >/dev/null
minio_shell mc pipe "local/${MINIO_BUCKET:-banking-lake}/${OBJECT_KEY}" <"$OBJECT_BACKUP" >/dev/null
minio_shell mc cat "local/${MINIO_BUCKET:-banking-lake}/${OBJECT_KEY}" >"${OBJECT_BACKUP}.restored"
[[ "$(sha256sum "${OBJECT_BACKUP}.restored" | awk '{print $1}')" == "$EXPECTED_OBJECT_SHA" ]]

echo "Phase 10 backup/restore drill passed: warehouse metadata/content fingerprint ${SOURCE_FINGERPRINT} and MinIO object SHA-256 ${EXPECTED_OBJECT_SHA} were restored; temporary targets will be removed."
