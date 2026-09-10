# Mango release compatibility

This release keeps YAML schema v3 execution semantics unchanged. The
compatibility gates below are the startup contract; the product build version
is diagnostic metadata and is not currently used as a SemVer major gate.

| Boundary | Supported contract | Failure behavior |
| --- | --- | --- |
| CLI ↔ daemon | IPC version `2` | `UNSUPPORTED_VERSION`; request version `0` is the v2 compatibility alias |
| daemon ↔ shim | Shim protocol `2`, bootstrap schema `1`, matching config fingerprint | `supervisor: shim` fails closed; no legacy fallback |
| YAML | Schema version `3` only | Reject the configuration; no automatic conversion |
| Metadata DB | Forward migration to schema `11` | A newer schema refuses daemon startup; downgrade is unsupported |
| HTTP | `/api/v1`, disabled by default, loopback by default | Other paths are rejected; non-loopback requires authentication |
| Build metadata | Go and Rust version, commit, build date | Uninjected local builds report `dev` / `unknown` |

Each packaged artifact contains `mango`, `mangod`, and `mango-shim` beside one
another. `manifest.json` records their hashes and the compatibility versions.
Windows packages use `.exe`; Linux and macOS packages use `tar.gz`.

The Phase 2 CLI/history cutover is intentionally coordinated with plan 06:

- `mango history` becomes `mango history ls`.
- `history clear` becomes `history purge ... --yes`.
- `mango schedule history` is removed.
- Retry creates a new run ID and records `retried_from_run_id`.
- `execution ls` defaults to queued/running; terminal history is separate.
