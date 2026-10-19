# Warehouse migrations

The executable Flyway migrations live in
`services/warehouse-writer/src/main/resources/db/migration`. Keeping the migration
with the owning warehouse schema makes the writer JAR self-contained. The Phase 8
batch module and Phase 9 analytics integration tests package the exact same directory
as a Maven resource, so all consumers validate one Flyway history instead of
maintaining copied DDL. Phase 9 adds indexes for bounded fraud range and account queries.
