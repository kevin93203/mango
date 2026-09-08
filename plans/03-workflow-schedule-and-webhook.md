# Phase 03: Workflow, Schedule, and Webhook

## Objective

Extend Mango's existing DAG and cron capabilities into reliable, event-aware
execution without turning Mango into a general-purpose distributed data
orchestrator.

## Scope

- Reliable DAG node execution.
- Node-level timeout, retry, cancellation, and failure policy.
- File-based artifact metadata.
- Schedule occurrences, timezone, DST, misfire, and catch-up handling.
- Optional secure webhook triggers.

## Dependencies

- Phase 01 unified execution and idempotency.
- Phase 02 accepted desired snapshots, reconciliation status, and service
  reliability.

## Public API / Configuration Changes

Add optional workflow node fields:

```yaml
workflows:
  release:
    tasks:
      deploy:
        uses: deploy
        needs: [test]
        timeout: 30m
        retry:
          retries: 2
          delay: 10s
        allow_failure: false
```

Add schedule misfire fields:

```yaml
schedules:
  - name: nightly
    cron: "0 2 * * *"
    timezone: Asia/Taipei
    target_type: workflow
    target: backup
    misfire: skip
    max_catch_up: 1
```

Supported misfire policies are `skip`, `run_once`, and `catch_up`. The default
is `skip`.

Add optional webhook definitions with path, target, and secret reference.

## Architecture Changes

Workflow nodes are persisted as child executions of one logical workflow run.
Node terminal states must distinguish `failed`, `skipped`, `cancelled`, and
`upstream_failed` semantics internally, while preserving compatible public
status output.

Artifacts are file metadata, not a large-object store. A task may declare
output paths; Mango validates and records existence, size, checksum, and path
relative to the project working directory.

Every cron activation receives a durable schedule occurrence ID. The scheduler
uses that ID and an idempotency key when creating the target execution.

Webhook delivery creates the same execution type as CLI and cron triggers.

## Implementation Tasks

- Add node-level timeout and retry override resolution.
- Add cancellation propagation from workflow to active nodes.
- Add `allow_failure` and explicit upstream failure propagation.
- Record node outputs and artifact metadata.
- Persist schedule occurrences before dispatching target runs.
- Implement misfire policy and bounded catch-up.
- Define DST behavior and test ambiguous/nonexistent local times.
- Add optional loopback webhook listener.
- Validate HMAC signature, timestamp, replay window, body size, and rate.
- Require an idempotency key for safe webhook deduplication.
- Add webhook and schedule events to execution history.

## Data Migration

- Add workflow-node policy fields as nullable metadata.
- Add schedule occurrence records without rewriting existing history.
- Keep existing schedules defaulting to `skip` and existing target-level retry
  behavior.
- Retain old trigger records and map them to the new occurrence model when
  possible.

## Test Plan

- Verify sequential and parallel DAG execution.
- Verify node timeout, retry, allow-failure, skipped, and cancellation states.
- Verify workflow status after upstream failure and partial cancellation.
- Verify artifact path validation, metadata recording, and missing outputs.
- Verify schedule occurrence uniqueness across daemon restart.
- Verify timezone, DST, misfire, catch-up, and catch-up limits.
- Verify valid and invalid webhook signatures.
- Verify replay rejection, duplicate idempotency keys, rate limiting, and body
  size limits.
- Verify webhook target validation and execution history linkage.

## Acceptance Criteria

- Every workflow node is individually observable and recoverable.
- A daemon restart cannot duplicate a scheduled execution.
- Webhooks can safely trigger tasks and workflows.
- The same webhook event cannot create duplicate logical runs.
- Existing v3 schedules continue to run with current default behavior.
- Workflow semantics remain simple, deterministic, and testable.

## Rollout Strategy

- Ship node-level policy and schedule occurrence persistence before enabling
  catch-up.
- Keep webhooks disabled unless explicitly configured.
- Bind the webhook listener to loopback by default.
- Roll out artifact recording without requiring artifact declarations.

## Out of Scope

- Arbitrary expression language.
- Dynamic task mapping or parameter matrices.
- Asset catalog and lineage system.
- Manual approval platform.
- Large artifact repository.
- Multi-host execution.
