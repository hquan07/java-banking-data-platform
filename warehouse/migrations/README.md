# Warehouse migrations

The executable Flyway migrations live in
`services/warehouse-writer/src/main/resources/db/migration`. Keeping the migration
with the owning application makes the packaged JAR self-contained and guarantees
that the same schema is applied in local Compose, tests, and deployments.
