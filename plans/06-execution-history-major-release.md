# Execution / History Major Release

This release separates the canonical execution store from the terminal-only
history read model. The existing `history.db` / `history.database` settings
and `history_*` table names remain unchanged.

## CLI changes

| Removed | Replacement |
| --- | --- |
| `mango history` | `mango history ls` |
| `mango history clear` | `mango history purge (--before RFC3339 \| --all) --yes` |
| `mango schedule history` | `mango history ls --trigger-type schedule` |
| `history ls --tail N` | `history ls --limit N` |
| `execution retry RUN_ID` reuses the run ID | creates a new run ID and records `retried_from_run_id` |

`execution ls` now lists `queued` and `running` by default. `--status STATUS`
selects one status and `--all` selects every status; the two flags are
mutually exclusive. Human-readable execution/history lists contain only
`RUN_ID`, target, status, started, and elapsed.

## IPC/API matrix

| Method | Major-release behavior |
| --- | --- |
| `execution.ls` | active-only by default; request adds `all` |
| `execution.retry` | creates a new execution and returns both run IDs |
| `history.ls` | terminal records only, newest-first |
| `history.get` | returns terminal run detail, nodes/tasks, and attempts |
| `history.purge` | deletes terminal metadata only; requires confirmation |
| `history.clear` | removed |
| `schedule.history` | removed |

The local IPC version is 2. A CLI and daemon with different protocol versions
fail with `UNSUPPORTED_VERSION` rather than silently interpreting requests
with different semantics.

## Data migration

Opening an existing database applies schema migration 3. It adds nullable,
indexed `history_runs.retried_from_run_id`; no foreign key is created, so a
source run may be purged later. Existing rows are read as-is. Historical
records that previously reused one `run_id` for retries are not split.

Retention and purge delete only terminal runs and their tasks, attempts,
events, and operations. They never delete queued/running metadata, lifetime
counters, or log files. Restore the existing `history.db.bak` backup procedure
if a SQLite migration fails.
