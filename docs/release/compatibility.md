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

The execution/history model remains an advanced compatibility surface while
plan 07 migrates users to the unified `runs` model:

- `mango project add` is retained as an alias for `mango project register`.
- `mango ps` and `mango ls` are retained as aliases for `mango service list`.
- `mango task ls` and `mango workflow ls` become hidden aliases for `list`.
- `mango task run` and `mango workflow run` are retained as aliases for
  `mango run task` and `mango run workflow`.
- `mango history` and `mango execution` remain callable but are hidden from
  the default root help; their leaf commands warn with the `mango runs`
  replacement.
- `history clear` becomes `history purge ... --yes`.
- `mango schedule history` is removed.
- Retry creates a new run ID and records `retried_from_run_id`.
- `mango runs list` includes active and terminal runs by default; `--active`
  is the explicit queued/running-only shortcut.
