# Mango YAML v4 migration

Mango v4 is a breaking project-schema release. It does not convert or modify
existing files. A v3 file is rejected with:

```text
configuration schema v3 is unsupported; Mango requires schema v4
```

Copy the file and update it explicitly before using `mango up`.

## Field changes

| v3 | v4 |
| --- | --- |
| `version: 3` | `version: 4` |
| project name stored only by `mango project add` | optional top-level `name`; otherwise directory name |
| task `env` | task `environment` |
| `workflows.<name>.tasks` | `workflows.<name>.steps` |
| workflow node `uses: task-name` | workflow node `task: task-name` |
| schedule `target_type` + `target` | schedule `run: task/<name>` or `run: workflow/<name>` |
| low-frequency fields under `defaults` | service-level fields or runtime defaults |

`healthcheck` is unchanged. Both `test` and multiple `checks`, including
`policy: all` and `policy: any`, keep their existing runtime behavior.

## Project lifecycle

The normal registration/apply sequence is now:

```sh
mango up
mango ps
mango down
```

`mango up` reads `./mango.yaml` by default. Use `--file PATH` for another
file and `--project NAME` to override the resolved name. The resolution order
is `--project`, YAML `name`, then the YAML directory name.

The older `mango project add/apply/plan/status/rollback` and service lifecycle
commands remain available as advanced operator interfaces. `mango down` keeps
the project registry, generation snapshots, logs, and execution history, so a
later `mango up` can restore and reconcile the project.
