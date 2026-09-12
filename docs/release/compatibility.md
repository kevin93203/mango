# Mango release compatibility

This release adopts YAML schema v4 as a deliberate breaking change. The
compatibility gates below are the startup contract; the product build version
is diagnostic metadata and is not currently used as a SemVer major gate.

| Boundary | Supported contract | Failure behavior |
| --- | --- | --- |
| CLI ↔ daemon | IPC version `3` | `UNSUPPORTED_VERSION`; request version `0` is the v3 compatibility alias; v2 is rejected |
| daemon ↔ shim | Shim protocol `3`, bootstrap schema `2`, matching config fingerprint and policy capabilities | `supervisor: shim` fails closed; no legacy fallback |
| YAML | Schema version `4` only | Reject v3 and older configurations; no automatic conversion |
| Metadata DB | Forward migration to schema `11` | A newer schema refuses daemon startup; downgrade is unsupported |
| HTTP | `/api/v1`, disabled by default, loopback by default | Other paths are rejected; non-loopback requires authentication |
| Build metadata | Go and Rust version, commit, build date | Uninjected local builds report `dev` / `unknown` |

Each packaged artifact contains `mango`, `mangod`, and `mango-shim` beside one
another. `manifest.json` records their hashes and the compatibility versions.
Windows packages use `.exe`; Linux and macOS packages use `tar.gz`.

This is a coordinated breaking upgrade. Stop the existing daemon with the
previous release's CLI before replacing the binaries. A v3 daemon refuses a
live v2 shim and reports the service that requires migration; dead v2 shim
state is safe to clean up. Do not start a v3 daemon against a v2 daemon or
shim and do not expect automatic fallback to the legacy supervisor.

The Stage 3 major release completes the CLI cutover to the unified `runs`
model. Removed command names are listed in
[the CLI migration guide](../migration-cli.md). Most removed names return a
non-zero migration error, write it to stderr, and do not contact the daemon.
The service namespace and the old `ps`/`ls` listing aliases are intentionally
unregistered and return an unknown-command error instead. None appear in help
or shell completion, and none preserve the old operation.

The data and protocol contract is unchanged by the CLI cleanup:

- Retry creates a new run ID and records `retried_from_run_id`.
- `mango runs list` includes active and terminal runs by default; `--active`
  is the explicit queued/running-only shortcut.
- Registry entries, generation snapshots, logs, history records, and run
  references remain readable after upgrade, downgrade, and rollback.
