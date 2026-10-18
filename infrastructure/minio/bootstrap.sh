#!/bin/sh
set -eu

alias_name=local
bucket=${MINIO_BUCKET:-banking-lake}
writer_user=${MINIO_ACCESS_KEY:-banking-lake-writer}
writer_secret=${MINIO_SECRET_KEY:-change-me-writer-local-only}
policy_name=banking-lake-writer
rendered_policy=/tmp/lake-writer-policy.json

case "$bucket" in
  ""|*[!a-z0-9.-]*)
    echo "MINIO_BUCKET must contain only lowercase letters, digits, dots, and hyphens" >&2
    exit 1
    ;;
esac

sed "s/__MINIO_BUCKET__/$bucket/g" /opt/banking/lake-writer-policy.json >"$rendered_policy"

until mc alias set "$alias_name" "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  sleep 1
done

mc mb --ignore-existing "$alias_name/$bucket"
mc anonymous set none "$alias_name/$bucket"
mc version enable "$alias_name/$bucket"

mc admin user add "$alias_name" "$writer_user" "$writer_secret" >/dev/null
mc admin policy create "$alias_name" "$policy_name" "$rendered_policy" >/dev/null
mc admin policy attach "$alias_name" "$policy_name" --user "$writer_user" >/dev/null

add_lifecycle_rule() {
  prefix=$1
  days=$2
  if ! mc ilm rule ls "$alias_name/$bucket" 2>/dev/null | grep -Fq "$prefix"; then
    mc ilm rule add --expire-days "$days" --prefix "$prefix" "$alias_name/$bucket"
  fi
}

add_lifecycle_rule bronze/ "${MINIO_BRONZE_RETENTION_DAYS:-30}"
add_lifecycle_rule silver/ "${MINIO_SILVER_RETENTION_DAYS:-90}"
add_lifecycle_rule gold/ "${MINIO_GOLD_RETENTION_DAYS:-365}"

mc ilm rule ls "$alias_name/$bucket"
