# ADR 0001: Restrict source-built MinIO Community Server to the local demo

- Status: Accepted
- Date: 2026-10-18
- Scope: Phase 6 local Docker Compose profile

## Context

Phase 6 needs an S3-compatible object store for reproducible Bronze/Silver/Gold acceptance tests. The pinned MinIO Community Server release is available as source, but the upstream repository is archived and the expected public container tags were not pullable during implementation. Using a floating or unofficial image would weaken reproducibility and provenance.

## Decision

Build `RELEASE.2025-10-15T17-29-55Z` directly from the official source tag in a pinned Go builder image. Build the matching command-line client from its pinned official source tag in the same image. Run the final image as non-root, bind API/console to loopback, keep the bucket private and versioned, and give `lake-writer` a dedicated bucket-scoped identity. Permit only synthetic data in this profile.

The image is a local portfolio dependency, not an approved production object store. Phase 6 tests must verify upload, stat/list, readback, checksum, lifecycle bootstrap and the manifest commit protocol.

## Consequences

- The first image build is slower because it compiles MinIO and `mc`; Docker cache makes later builds faster.
- The build still depends on the pinned tags remaining obtainable from the official archived repositories.
- Local HTTP and an unencrypted named volume are accepted only because access is loopback-only and data is synthetic.
- Before any non-local deployment, select or reassess a maintained S3-compatible store, test layout/API compatibility and migration, and implement TLS, external secrets, KMS-backed server-side encryption, audit logging, backup/restore and vulnerability management.
- A future replacement must preserve deterministic object keys and manifest semantics or provide a documented migration.
