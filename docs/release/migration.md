# Mango metadata migration guide

The current metadata schema is version 11. Migration steps are explicit and
named; they are not production `AutoMigrate` operations. Every step is
idempotent, runs in its own transaction where the database supports that
guarantee, and writes a fixed checksum to `schema_migrations`.

## IPC and shim policy migration

IPC v3 and bootstrap schema v2 are a coordinated breaking release. Before
replacing binaries, use the old `mango` CLI to stop `mangod` and all services.
Then install the matching `mango`, `mangod`, and `mango-shim` artifacts and
start the daemon. A live v2 shim is rejected with an actionable migration
error; dead v2 state is removed during reconciliation. The v3 shim never
falls back to the legacy supervisor when secret, identity, or resource policy
application fails.

## SQLite

For `MANGO_HOME/state/history.db`, the runner uses:

```text
history.db.migration.lock
history.db.migration.json
history.db.migration-<run-id>.bak
history.db.bak
```

`history.db.bak` is a compatibility backup and is never overwritten. An
actual upgrade creates the run-specific backup, writes its SHA-256 into the
marker, and atomically updates the marker with mode `0600`. A fresh empty
database does not create a needless backup.

The marker records source and target schema versions, the last completed step,
source and backup checksums, timestamps, and the last error. A `running` or
`failed` marker can be retried when the database and marker are consistent. If
the backup is missing, either checksum differs, or the schema version does not
match the marker progress, startup is blocked.

Migration happens before the IPC listener, PID file, scheduler, or
reconciliation are started. Do not edit or delete the database or marker while
recovering a failure.

## PostgreSQL and MySQL

PostgreSQL uses an advisory lock and can run a migration in a transaction.
MySQL uses `GET_LOCK` / `RELEASE_LOCK`; because DDL can implicitly commit,
each step must remain idempotent. The operator must complete an external
database backup before starting an upgrade. Mango stores the migration ledger
in `schema_migrations`, but does not create file backups or automatically
rollback a failed migration.

## Inspection

When the daemon is stopped, use:

```sh
mango doctor --json
```

The database report includes migration status, current/target schema, marker
and backup paths, checksum mismatch, the last error, and a recovery action.
Inspection opens SQLite read-only and never runs a migration.
