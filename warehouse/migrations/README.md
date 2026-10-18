# Warehouse migrations

The executable Flyway migrations live in
`services/warehouse-writer/src/main/resources/db/migration`. Keeping the migration
with the owning warehouse schema makes the writer JAR self-contained. The Phase 8
batch module packages the exact same directory as a Maven resource, so both
executables validate one Flyway history instead of maintaining copied DDL.
