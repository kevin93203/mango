# Mango release checklist

## Build and package

- [ ] Build on native Windows, Linux, and macOS runners.
- [ ] Inject the same version, commit, and UTC build date into all three binaries.
- [ ] Package `mango`, `mangod`, `mango-shim`, `manifest.json`, `README.md`, and
      `mango.example.yaml` at one archive level.
- [ ] Recompute every binary and archive SHA-256 from the package.
- [ ] Verify all three `--version` commands and sibling executable discovery.

## Compatibility and migration

- [ ] Verify the coordinated IPC v3, shim protocol v3, and bootstrap schema v2
      contract; v2 clients and live shims are rejected with migration guidance.
- [ ] Stop the old daemon with the old CLI before installing the new package;
      confirm dead v2 shim state is cleaned and no legacy fallback occurs.
- [ ] Confirm YAML v4 starter and advanced fixtures validate; v3 is rejected
      without modifying the source file.
- [ ] Test SQLite fresh creation and upgrades from schema 1, 3, 9, and 10.
- [ ] Test migration lock, marker retry, stale marker, missing backup, checksum
      mismatch, and newer-schema refusal.
- [ ] Confirm old history rows, registry JSON, and legacy state remain readable.
- [ ] For PostgreSQL/MySQL, complete an external backup and review the
      idempotent DDL plan before migration.

## Smoke and rollout

- [ ] Start the packaged daemon with a temporary `MANGO_HOME` and inspect
      `mango doctor --json`.
- [ ] Verify project apply, service lifecycle, task/watch/log, workflow,
      artifact metadata, history, schedules, webhooks, events, audit, metrics,
      and `/api/v1/health`.
- [ ] Verify shim start, daemon restart, reattach, and fail-closed mismatch.
- [ ] Verify secret references and cross-chunk stdout/stderr redaction, POSIX
      normalized `run_as`, and tree-wide resource enforcement on each supported
      platform; configured macOS resources are rejected.
- [ ] Verify startup integration status/install/uninstall or an explicit
      unsupported/degraded result on the platform.
- [ ] Publish the migration guide, rollback guide, and this checklist with the
      artifact.

## Fixed rollout order

1. Phase 00 capability and test improvements.
2. Compatibility-mode unified execution and persistence.
3. Plan/diff and configuration rollback, then health-triggered restart.
4. Workflow and schedule enhancements, retaining `misfire: skip` defaults.
5. Webhook, secret reference, resource limit, events, and metrics opt-ins.
