# Mango Engineering Plans

## Product Positioning

Mango is a cross-platform, process-native service and workflow manager for
executable applications that do not run in containers.

It manages long-running services, one-off tasks, DAG workflows, health,
restart recovery, logs, execution history, and schedules on a single host.
Mango does not provide container isolation, OCI images, registry management,
Kubernetes orchestration, or multi-host high availability.

## Plan Status

| Plan | Status | Depends on |
| --- | --- | --- |
| [00 Foundation and Platform](00-foundation-and-platform.md) | Completed | None |
| [01 Unified Execution and Persistence](01-unified-execution-and-persistence.md) | Completed | 00 |
| [02 Desired State and Service Reliability](02-desired-state-and-service-reliability.md) | Completed | 01 |
| [03 Workflow, Schedule, and Webhook](03-workflow-schedule-and-webhook.md) | Completed | 01, 02 |
| [04 Security, Resources, and Observability](04-security-resources-and-observability.md) | Completed | 01, 02, 03 |
| [05 Release, Migration, and Rollout](05-release-migration-and-rollout.md) | Completed | 00–04 |
| [06 Execution / History Major Release](06-execution-history-major-release.md) | Completed | 01, Cobra migration |
| [07 CLI Simplification and Progressive Migration](07-cli-simplification-and-migration.md) | Stage 3 in progress | 00–06 |

## Implementation Audit

- **Phase 00 — Completed.** Added the platform-neutral Go and Rust test
  fixtures, capability discovery and reporting through `mango doctor` and the
  health API, direct-executable integration coverage, cross-platform process
  tree/startup checks, CI acceptance steps, and documentation of the local IPC
  ownership, timeout, versioning, and shim attach/recovery boundaries. Existing
  YAML v3 and local-only IPC behavior remain unchanged.
- **Phase 01 — Completed.** Durable `run_id` records, queued/running/terminal
  lifecycle transitions, attempts, parent and trigger metadata, configuration
  generations, concurrent idempotency, execution query/watch/cancel/retry/log
  IPC, event and operation read-back, versioned metadata migrations with SQLite
  backup and fail-closed behavior, and active-run interruption recovery are
  implemented. Workflow parent/child linkage, cancellation cleanup, terminal
  history compatibility, post-exit logs, migration reopen/backup/failure, and
  daemon restart recovery are covered by tests. Existing YAML v3 projects and
  completed history remain readable.
- **Phase 02 — Completed.** Uses one desired/observed state model for plan
  and apply, plan v2 resources across services/tasks/workflows/schedules,
  immutable accepted snapshots, asynchronous latest-generation-wins
  reconciliation, readiness status, retry backoff, and accepted-state
  rollback. Runtime failures no longer roll back the registry pointer. Existing
  YAML v3 projects and committed generation snapshots remain readable; legacy
  apply-operation metadata is ignored and is not removed automatically.
  `go test ./...`, `go test -race ./...`, `go vet ./...`, and `go build ./...`
  pass.
- **Phase 04 — Completed.** Added scalar-or-reference environment decoding
  with `from_env` and `from_file` providers, runtime-only secret resolution,
  redaction wrappers for process output and structured daemon logs, optional
  `run_as` identity handling, capability-aware resource policy reporting,
  and guarded shim bootstrap handling without persisting secret values; service
  policies that require daemon-side redaction or identity/resource adapters
  are rejected for shim-supervised services.
  Added durable version-10 `events`, `audit_entries`, `secret_references`,
  and `resource_policies` metadata tables, a non-blocking event bus, event
  retention, administrative audit records, Prometheus text metrics, and
  correlation fields for operation/run/configuration generation metadata.
  Added `mango events` and `mango status --watch`, versioned opt-in HTTP
  endpoints under `/api/v1`, bearer-token authentication for configured or
  non-loopback listeners, and explicit resource capability status. Existing
  string environment values, webhook HMAC behavior, local IPC permissions,
  YAML v3, and execution-history records remain compatible. Go and Rust
  formatting, unit/integration tests, and static checks pass.
- **Phase 05 — Completed.** Added explicit schema-versioned migration
  coordination through metadata schema 11, SQLite migration locking,
  run-specific backups, atomic markers, checksum validation, fail-closed
  recovery, and read-only `mango doctor` inspection. Added shared Go/Rust build
  metadata, health reporting, registry preservation backups, compatibility and
  rollback documentation, and a native Windows/Linux/macOS artifact workflow
  that packages the CLI, daemon, and shim together with a manifest and
  checksums. Packaged-binary smoke verification covers version output,
  sibling resolution, YAML v3 validation, daemon startup, health, and clean
  shutdown; broader feature coverage remains in the existing cross-platform
  integration suite.
- **Phase 07 — Stage 3 in progress.** The canonical root service, project,
  task, workflow, schedule, and unified run commands are now the executable CLI
  surface. Most removed names fail with stderr migration errors; the service
  namespace and `ps`/`ls` aliases are unregistered, stay out of help and
  completion, and do not contact the daemon.

## Dependency Flow

```text
00 Foundation
      ↓
01 Execution / Persistence
      ↓
02 Desired State / Service Reliability
      ↓
03 Workflow / Schedule / Webhook
      ↓
04 Security / Resources / Observability
      ↓
05 Release / Migration / Rollout
      ↓
06 Execution / History Major Release
      ↓
07 CLI Simplification / Major-release Cleanup
```

## Shared Decisions

- Existing YAML version 3 remains compatible. The Cobra-equivalence release
  preserved the public CLI; the Stage 2 and Stage 3 releases moved run queries
  to `runs` and removed the legacy command names listed in plan 07.
- Unix domain sockets and Windows named pipes remain the default local control
  transport.
- HTTP is optional, loopback-only by default, and versioned under `/api/v1`.
- Webhooks use HMAC signatures, timestamps, replay protection, and idempotency
  keys. TLS termination is provided by an external reverse proxy.
- Persistence is hybrid:
  - YAML and the project registry own desired configuration.
  - Runtime files and `mango-shim` state own process ownership and observed
    process state.
  - The metadata database owns executions, events, audit records,
    and schedule occurrences.
  - Log files own execution output.
- Mango remains single-host and process-native. A future remote-control design
  must not be introduced implicitly by these plans.

## Shared Quality Gate

Every plan that changes behavior must include unit tests, cross-platform tests
where applicable, failure recovery tests, documentation updates, and a clear
migration or compatibility statement.

The repository must pass:

```text
go test ./...
go test -race ./...
go vet ./...
cargo fmt --manifest-path mango-shim/Cargo.toml -- --check
cargo test --manifest-path mango-shim/Cargo.toml
cargo clippy --manifest-path mango-shim/Cargo.toml --all-targets -- -D warnings
```

## Recommended Execution Order

1. Complete platform-neutral test infrastructure and record current
   capabilities.
2. Introduce persistent execution identity and metadata without changing the
   existing v3 configuration contract.
3. Add plan/apply reliability and health actions.
4. Add richer DAG semantics, schedule occurrence handling, and webhooks.
5. Add secrets, resource controls, events, metrics, and auditability.
6. Perform migration, release, upgrade, and rollback validation.
7. Complete the Stage 3 CLI removal, migration-error, completion, and
   packaged smoke-test gates.
