# Phase 04: Security, Resources, and Observability

## Objective

Make Mango safe to operate in enterprise environments while preserving its
process-native and single-host scope.

## Scope

- Secret references and redaction.
- Local IPC and optional HTTP security.
- User/group execution.
- Capability-based process, CPU, and memory limits.
- Event stream, metrics, structured logs, correlation, and audit.

## Dependencies

- Phase 01 execution and event records.
- Phase 02 service reconciliation and health transitions.
- Phase 03 webhook and schedule events.
- Phase 00 capability model.

## Public API / Configuration Changes

Support environment values and references without invalidating existing string
values:

```yaml
environment:
  APP_ENV: production
  API_TOKEN:
    from_env: MANGO_API_TOKEN
```

The first provider implementations are `from_env` and `from_file`. A provider
interface must be available for future secret stores.

Add optional service policy:

```yaml
services:
  worker:
    run_as:
      user: mango
      group: mango
    resources:
      process_limit: 100
      memory: 512MiB
      cpu_percent: 80
```

Add optional versioned HTTP endpoints under `/api/v1`. HTTP is disabled unless
configured and binds to loopback by default.

Add CLI commands:

```text
mango events
mango events --follow
mango status --watch
```

## Architecture Changes

Use one event model for service, execution, schedule, configuration, webhook,
health, and audit transitions. Events include timestamp, actor, project,
target, run ID, operation ID, configuration generation, and safe metadata.

The HTTP API mirrors versioned local operations but is not a public network
control plane. Local IPC remains protected by Unix file permissions and
Windows named-pipe security descriptors. Webhooks use HMAC independently from
administrative API access.

Resource limits are capability-based:

```text
Linux:   process groups / cgroup v2 where available
Windows: Job Object where available
macOS:   report unsupported limits explicitly
```

Mango remains clear that these controls are not container isolation or a
security sandbox.

## Implementation Tasks

- Add scalar-or-reference environment decoding with strict validation.
- Add secret provider interface and environment/file providers.
- Redact secret values from history, logs, CLI output, TUI, and API metadata.
- Add service user/group resolution and validation.
- Add process count, memory, and CPU limit adapters.
- Expose supported, unsupported, and degraded resource status.
- Add event bus and durable event records.
- Add Prometheus-compatible metrics.
- Add structured daemon and execution logs with run/operation correlation.
- Add administrative audit entries with actor, action, target, result, and time.
- Add optional loopback HTTP API and authentication configuration.

## Data Migration

- Add event, audit, secret-reference, and resource-policy metadata tables.
- Do not migrate secret values into the database.
- Preserve existing plain environment strings and their current behavior.
- Apply restrictive permissions to new token, state, and audit files.
- Keep event retention configurable and separate from execution-history
  retention.

## Test Plan

- Verify secret values never appear in logs, history, JSON, TUI, or errors.
- Verify environment and file references resolve at execution time.
- Verify missing, unreadable, and malformed secret references fail safely.
- Verify IPC permission and HTTP loopback behavior.
- Verify resource limits on supported platforms and explicit unsupported output
  elsewhere.
- Verify event ordering, correlation IDs, run IDs, and operation IDs.
- Verify audit records for apply, lifecycle, execution, schedule, and webhook
  operations.
- Verify metrics for service, health, execution, schedule, and log behavior.

## Acceptance Criteria

- Secret values are not persisted or displayed accidentally.
- Resource behavior is explicit on every supported platform.
- Administrative actions are auditable.
- Operators can follow execution and service state without polling every
  individual object.
- Metrics and structured events identify the affected run, service, or
  operation.
- HTTP is opt-in and not exposed beyond loopback by default.

## Rollout Strategy

- Enable secret references without changing existing string environment
  behavior.
- Ship read-only events and metrics before enabling HTTP mutations.
- Roll out resource limits per platform capability.
- Treat unsupported resource settings as validation errors when explicitly
  configured.

## Out of Scope

- Full RBAC and multi-tenant authorization.
- Public internet-facing API.
- Built-in TLS certificate management.
- Vault or cloud secret-store integrations in the first implementation.
- Container namespaces, seccomp, AppArmor, or sandbox isolation.
- Multi-host resource scheduling.
