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
| [02 Desired State and Service Reliability](02-desired-state-and-service-reliability.md) | Not started | 01 |
| [03 Workflow, Schedule, and Webhook](03-workflow-schedule-and-webhook.md) | Not started | 01, 02 |
| [04 Security, Resources, and Observability](04-security-resources-and-observability.md) | Not started | 01, 02, 03 |
| [05 Release, Migration, and Rollout](05-release-migration-and-rollout.md) | Not started | 00–04 |
| [06 Execution / History Major Release](06-execution-history-major-release.md) | In progress | 01, Cobra migration |

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
```

## Shared Decisions

- Existing YAML version 3 remains compatible. The Cobra-equivalence release
  preserves the public CLI; the Phase 2 major release intentionally changes
  execution/history defaults and removes the commands listed in plan 06.
- Unix domain sockets and Windows named pipes remain the default local control
  transport.
- HTTP is optional, loopback-only by default, and versioned under `/api/v1`.
- Webhooks use HMAC signatures, timestamps, replay protection, and idempotency
  keys. TLS termination is provided by an external reverse proxy.
- Persistence is hybrid:
  - YAML and the project registry own desired configuration.
  - Runtime files and `mango-shim` state own process ownership and observed
    process state.
  - The metadata database owns executions, operations, events, audit records,
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
