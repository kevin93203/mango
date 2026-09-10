# Mango rollback and recovery guide

Configuration rollback and database rollback are separate operations.
`project rollback` restores an accepted configuration generation only; it
never deletes history and never downgrades the metadata schema.

## Failed SQLite migration

1. Stop `mangod` and leave the database and marker untouched.
2. Run `mango doctor --json` and inspect `database.migration`.
3. If the marker is consistent, retry `mangod`; migration steps are idempotent.
4. If the status is blocked, verify the marker's `backup_path` and
   `backup_sha256`. Copy the verified backup over `history.db` using an
   operator-approved, atomic file replacement, then retry.
5. Keep the failed database, marker, and logs for incident analysis.

Never use `project rollback` as a database rollback. Never remove
`schema_migrations` to bypass a checksum check.

## External databases

Restore PostgreSQL or MySQL from the operator's backup when a migration cannot
be completed. Mango does not issue an automatic downgrade or rollback. Start
the daemon only after the restored database is at a schema supported by the
binary.

## Binary rollback

Use a package whose manifest declares a compatible IPC, shim, YAML, and
metadata-schema contract. A binary that understands an older schema cannot
start against a newer database; restore the database backup first.
