# Mango

Mango is a cross-platform service and workflow manager written in Go. It runs
long-lived processes, executes one-off tasks, connects tasks into workflows,
and triggers them on cron schedules or optional authenticated webhooks.

Mango manages any executable. It does not require Node.js, containers, or a
shell-based process definition.

## Contents

- [What Mango provides](#what-mango-provides)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Command overview](#command-overview)
- [Complete YAML reference](#complete-yaml-reference)
- [Complete CLI reference](#complete-cli-reference)
- [Repository examples](#repository-examples)
- [Runtime files](#runtime-files)
- [Platform behavior](#platform-behavior)
- [Security and observability](#security-and-observability)
- [Release, migration, and rollback](#release-migration-and-rollback)
- [YAML v4 migration](docs/migration-v4.md)
- [Development](#development)

## What Mango provides

- Service lifecycle management: start, stop, restart, enable, and disable.
- Crash recovery with restart policies, backoff, and crash-loop protection.
- Service dependencies with started, healthy, and completed-successfully conditions.
- Health checks, process-tree inspection, CPU/RSS metrics, uptime, and ports.
- Separate stdout and stderr logs with size-based rotation and retention.
- One-off tasks with timeouts, retries, and concurrency control.
- DAG workflows that run task nodes sequentially or in parallel.
- Five-field cron schedules with IANA time zones.
- Optional authenticated loopback webhooks for triggering tasks and workflows,
  with HMAC signatures, replay protection, rate limiting, and idempotency.
- A terminal monitor and interactive execution-history browsers.
- Per-user startup integration on Windows, Linux, and macOS.
- JSON output for automation and CI where supported.
- Runtime secret references with redacted output and history-safe metadata.
- Durable events, administrative audit records, structured daemon logs, and
  Prometheus-compatible metrics.
- Optional authenticated `/api/v1` HTTP administration, disabled by default.

## How it works

Mango has a small client/daemon architecture:

```text
                    local IPC
  mango CLI  -------------------------->  mangod
     |                                      |
     |                                      +-- config/dependencies/health
     |                                      +-- task/workflow executor
     |                                      +-- cron scheduler
     |                                      +-- shim client/reconciliation
     |                                             |
     |                                      mango-shim (one per shim service)
     |                                             |
     |                                      service process tree + logs
     |
     +-- project registry and YAML configuration
```

- `mango` is the command-line client.
- `mangod` is the background daemon. It owns configuration, dependencies,
  health, tasks, workflows, schedules, and reconciliation.
- `mango-shim` is a small per-service supervisor used when a service opts into
  `supervisor: shim`. It owns that service's process tree, logs, restart policy,
  and durable state independently of `mangod`.
- The client and daemon communicate through a Unix domain socket on Unix-like
  systems and a Windows named pipe on Windows.
- Each `MANGO_HOME` is one daemon instance. `mangod` takes an OS-level lock at
  `MANGO_HOME/runtime/daemon.lock`; different `MANGO_HOME` values can run in
  parallel.
- A project is a name attached to one YAML configuration file. Use the YAML
  `name`, or let Mango derive it from the directory name; `--project` can
  explicitly override it.

The main implementation areas are organized under `internal/`:

| Package | Responsibility |
| --- | --- |
| `config` | YAML v4 loading, validation, and default resolution |
| `daemon` | Project reconciliation, IPC handling, and service supervision |
| `mango-shim/` | Rust per-service process-tree supervision, restart, logs, and durable state |
| `process` | Cross-platform process and process-tree control |
| `scheduler` / `workflow` | Cron execution, task runs, DAGs, retries, and history |
| `health` / `metrics` / `logging` | Probes, process metrics, and rotating logs |
| `registry` / `startup` / `tui` | Project registry, OS startup integration, and terminal UI |

## Requirements

- Go 1.23 or newer.
- Stable Rust toolchain for building `mango-shim`.
- Windows 10/11, Linux, or macOS.
- The executables referenced by your configuration must be available on the
  host. The canonical examples use the Go toolchain, and native HTTP
  healthchecks do not require an external probe executable.

## Quick start

Build the CLI, daemon, and shim together:

macOS/Linux:

```sh
mkdir -p bin
go build -o bin/mango ./cmd/mango
go build -o bin/mangod ./cmd/mangod
cargo build --release --manifest-path mango-shim/Cargo.toml
cp mango-shim/target/release/mango-shim bin/mango-shim
```

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force bin
go build -o bin/mango.exe ./cmd/mango
go build -o bin/mangod.exe ./cmd/mangod
cargo build --release --manifest-path mango-shim/Cargo.toml
Copy-Item mango-shim/target/release/mango-shim.exe bin/mango-shim.exe
```

Start the daemon explicitly, then generate and start the example project.
`mango up` connects to the running daemon, registers the YAML, applies it, and
waits for services to become ready:

```sh
./bin/mango init
./bin/mango daemon start
./bin/mango up
./bin/mango ps
./bin/mango down
```

In Windows PowerShell, replace `./bin/mango` with `.\bin\mango.exe`.
The default file is `./mango.yaml`; use `--file PATH` for another file and
`--project NAME` to override the project name.

Inspect the sample service and its logs:

```sh
./bin/mango status demo/api
./bin/mango logs demo/api --stream all --tail 50
./bin/mango logs demo/api --follow
```

`mango up` does not start the daemon automatically. To run the daemon in the
background, use `mango daemon start`. This command expects `mangod` next to
`mango` or somewhere on `PATH`.

The starter configuration contains one service, one task, one schedule, and a
single healthcheck. See
[`mango.example.yaml`](mango.example.yaml) and
[`examples/tasks/README.md`](examples/tasks/README.md) for guided examples.
The advanced schema example is [`docs/advanced-example.yaml`](docs/advanced-example.yaml).
The canonical examples use Go source files and are intended to run on
Windows, macOS, and Linux. The starter HTTP healthcheck is native and does
not require `curl`.

## Configuration

Mango configuration files use YAML schema version 4. Only `.yaml` files are
supported:

```yaml
version: 4
name: demo

defaults:
  working_dir: .
  restart: on-failure
  environment:
    APP_ENV: development

services:
  api:
    command: go
    args: [run, ./examples/api/main.go, --port, "8080"]
    autostart: true
    healthcheck:
      test: [HTTP, http://127.0.0.1:8080/]

tasks:
  cleanup:
    command: ./bin/cleanup
    timeout: 5m
    retry:
      retries: 2
      delay: 10s

workflows:
  nightly:
    concurrency: forbid
    steps:
      clean:
        task: cleanup

schedules:
  - name: nightly
    cron: "0 2 * * *"
    timezone: Asia/Taipei
    run: workflow/nightly
```

### Compose-like project lifecycle

```text
mango up [--project NAME] [--file PATH]
mango ps [--project NAME] [--file PATH]
mango down [--project NAME] [--file PATH]
```

With no `--file`, Mango reads `./mango.yaml`. Project resolution is
`--project`, then YAML `name`, then the YAML directory name. The daemon must
already be running; `up` registers or updates the project, applies the
configuration, enables schedules, starts autostart services, and waits up to
60 seconds for readiness. If the effective configuration is unchanged, apply
reuses the current generation and does not restart services. A readiness
timeout returns a non-zero status while background reconciliation continues.

`down` stops services, disables schedules, and marks the project disabled. It
does not delete YAML, registry data, generations, logs, or execution history.
It can use a registered project with `--project` even when the YAML is
currently invalid. `ps` shows the compact service state table; `ls` remains an
alias.

### Services

Services represent long-running processes. A service command is executed
directly, without shell parsing. Use the shell executable explicitly when you
need shell features such as pipes or redirection.

Common service settings include:

- `command`, `args`, `working_dir`, and `environment`.
- `supervisor`: `legacy` or `shim`; the default is `legacy` during rollout.
- `autostart` when the daemon starts or a project is applied.
- `restart`: `never`, `on-failure`, or `always`.
- `startup_timeout`, `stop_timeout`, restart limits, and crash-loop protection.
- `healthcheck` and `depends_on`.
- `run_as.user` / `run_as.group` for host identity execution where supported.
- `resources.process_limit`, `resources.memory`, and `resources.cpu_percent`
  for capability-aware host resource controls.

Health is reported separately from the service lifecycle. A failed health check
does not automatically restart the service unless `healthcheck.on_unhealthy` is
set to `restart` or `stop`.

### Tasks

Tasks are one-off command executions. They support their own working directory,
environment (`environment`), timeout, retry policy, and concurrency mode:

- `forbid` prevents overlapping runs of the same task.
- `allow` permits overlapping runs.

### Workflows

Workflows are directed acyclic graphs of task nodes. Each node uses a task from
the same project and can declare `needs` dependencies. Independent nodes may
run in parallel; downstream nodes are skipped when a required upstream node
fails or is skipped.

### Schedules

Schedules are cron triggers. Each schedule targets either one task or one
workflow using `target_type` and `target`. Cron expressions use five fields and
the optional `timezone` value must be an IANA time zone.

### Secrets and references

Environment values may remain plain strings or use a runtime-only reference:

```yaml
services:
  worker:
    command: ./bin/worker
    environment:
      APP_ENV: production
      API_TOKEN:
        from_env: MANGO_API_TOKEN
      TLS_PASSWORD:
        from_file: C:/secure/mango/tls-password
```

The first providers are `from_env` and `from_file`. Values are resolved when a
service or task starts, are never copied into the metadata database, and are
redacted from process output, structured logs, errors, and control-plane
metadata. The file provider trims only the final newline and fails closed for
missing, unreadable, or empty files. Webhook `secret_ref` accepts `env:NAME`
and `file:PATH`.

### Resource policies

```yaml
services:
  worker:
    command: ./bin/worker
    run_as:
      user: mango
      group: mango
    resources:
      process_limit: 100
      memory: 512MiB
      cpu_percent: 80
```

Mango reports each configured policy as `supported`, `degraded`, or
`unsupported` in service status and `mango doctor`. Linux requires delegated
cgroup v2 controllers and applies `pids.max`, `memory.max`, and `cpu.max` to
the complete service tree. Windows uses Job Object active-process, memory, and
CPU hard-cap limits. macOS rejects configured resource policies as unsupported.
These controls are resource boundaries, not container isolation or a security
sandbox; a policy that cannot be applied prevents the service from starting.
Both supervisors resolve `from_env`/`from_file` secret references at spawn,
redact child stdout/stderr, and apply POSIX `run_as`; Windows `run_as` remains
unsupported.

## Command overview

The Cobra command tree is the authoritative source for command syntax and
flags. Use `mango --help` or `mango COMMAND --help` for the current interface;
the examples below focus on common workflows rather than duplicating every
generated help entry.

The general form is:

```text
mango <command> [subcommand] [arguments] [options]
```

### Projects and configuration

```sh
mango init [PATH] [--force]
```

Creates a complete example configuration and copies the Go source files it
uses into `examples/` beside the YAML. The default output path is
`./mango.yaml`; a parent directory is created when needed. Existing files are
not overwritten unless `--force` is provided. `PATH` must use the `.yaml`
extension. `init` does not register or apply a project.

```sh
mango init
mango init ./config/mango.yaml
mango init ./mango.yaml --force
```

Validate and register the generated configuration separately:

```sh
mango config validate PATH
mango project add NAME PATH
mango project plan PROJECT
mango project status PROJECT [--json]
mango project apply NAME [--wait]
mango project rollback PROJECT [GENERATION] [--wait]
mango project ls
mango project rename OLD NEW
mango project remove NAME
```

`project add` stores the absolute YAML path in the registry. It does not copy
or modify the configuration file. `project plan` explicitly reads the YAML and
previews the changes. `project apply` accepts a validated and compiled desired
snapshot when the effective configuration changes, then reconciles the daemon
in the background. An unchanged effective configuration reuses the current
generation. Use `project status` to inspect convergence; `--wait` waits for
readiness without a timeout. A daemon restart restores the latest accepted
generation; editing the YAML alone does not change running or restored
services.

### Services

```sh
mango up [--project NAME] [--file PATH]
mango ps [--project NAME] [--file PATH]
mango down [--project NAME] [--file PATH]
mango ls
mango status PROJECT/SERVICE
mango start PROJECT/SERVICE
mango stop PROJECT/SERVICE
mango restart PROJECT/SERVICE
mango enable PROJECT/SERVICE
mango disable PROJECT/SERVICE
```

Lifecycle commands also accept a project name to operate on all services in
that project, a numeric service ID, or multiple targets. Service IDs are
convenient for a running daemon but can change after a daemon restart; project
and service names are the stable form.

### Logs and monitoring

```sh
mango logs PROJECT/SERVICE --stream all --tail 15
mango logs PROJECT/SERVICE --follow
mango logs clear PROJECT/SERVICE
mango monitor
mango daemon logs --follow
```

Task logs use `PROJECT/task/TASK`. Workflow-node logs use
`PROJECT/workflow/WORKFLOW/NODE`.

### Tasks, workflows, and schedules

```sh
mango task ls
mango task run PROJECT/TASK

mango workflow ls
mango workflow run PROJECT/WORKFLOW

mango schedule ls
mango schedule enable PROJECT|PROJECT/SCHEDULE [PROJECT|PROJECT/SCHEDULE ...]
mango schedule disable PROJECT|PROJECT/SCHEDULE [PROJECT|PROJECT/SCHEDULE ...]

mango history ls
mango history show RUN_REF
mango history purge (--before RFC3339 | --all) --yes
```

`task` and `workflow` are execution targets. `schedule`, `webhook`, and
`manual` are trigger sources. Webhook delivery is available when the optional
listener is enabled in `daemon.yaml`; otherwise use `task run` or `workflow
run` for a manual run.
`history` is the terminal-only execution-history browser. Use `--json` for
machine-readable history and status data.

## Complete YAML reference

### File rules

- Configuration files must use the `.yaml` extension.
- `version` must be `3`; older versions are rejected and are not converted.
- Unknown YAML fields are rejected, so misspelled options fail validation.
- A file must define at least one service, task, workflow, or schedule.
- Names use letters, digits, `.`, `_`, and `-`, and must start with a letter or
  digit. Project names are stricter: they must start with an ASCII letter.
- Relative paths are resolved from the directory containing the YAML file.

Validate a file before registering or applying it:

```sh
mango config validate ./mango.yaml
```

### Top-level fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `version` | integer | yes | Must be `4`. |
| `name` | string | no | Project name. Overridden by `--project`; otherwise derived from the YAML directory. |
| `defaults` | mapping | no | Defaults shared by services and tasks. |
| `services` | mapping | no | Long-running processes keyed by service name. |
| `tasks` | mapping | no | One-off commands keyed by task name. |
| `workflows` | mapping | no | DAGs of task nodes keyed by workflow name. |
| `schedules` | sequence | no | Cron triggers targeting a task or workflow. |
| `webhooks` | sequence | no | Authenticated HTTP triggers targeting a task or workflow. |

### `defaults`

```yaml
defaults:
  working_dir: .
  restart: on-failure
  environment:
    APP_ENV: development
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `working_dir` | string | `.` | Base working directory for services and tasks. Relative paths are resolved from the YAML file directory. |
| `restart` | string | `on-failure` | Service restart policy: `never`, `on-failure`, or `always`. |
| `environment` | string-or-reference map | inherited environment | Variables shared by services and tasks. |

`defaults` intentionally contains only high-frequency shared settings. Keep
low-frequency controls such as timeouts, log rotation, metrics, and
crash-loop limits at the service level. Service fields override defaults.
Tasks use `working_dir` and `environment` but do not use service restart
settings.

### `services`

Each key under `services` is a service name:

```yaml
services:
  api:
    command: go
    args: [run, ./examples/api/main.go, --port, "8080"]
    working_dir: .
    environment:
      APP_ENV: development
    autostart: true
    restart: always
    stop_timeout: 15s
    max_restarts: 5
    restart_window: 2m
    stable_after: 30s
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `<service-name>` | mapping key | — | Unique service name within the project. |
| `command` | string | — | Required executable name or path. It is executed directly, without a shell. |
| `supervisor` | string | `legacy` | `legacy` uses the daemon-owned backend; `shim` delegates process-tree ownership to a per-service `mango-shim`. |
| `args` | sequence of strings | `[]` | Arguments passed to `command`. |
| `working_dir` | string | `defaults.working_dir` or `.` | Working directory for the process. |
| `environment` | string-or-reference map | inherited environment | Variables to add or override. References use `from_env` or `from_file` and resolve only at process start. |
| `autostart` | boolean | `false` | Start when the daemon starts or the project is applied. |
| `restart` | string | `defaults.restart` or `on-failure` | `never`, `on-failure`, or `always`. |
| `startup_timeout` | duration | `30s` | Maximum time to reach a healthy state when a healthcheck is configured. |
| `stop_timeout` | duration | `10s` | Graceful-stop timeout. |
| `max_restarts` | integer | `10` | Restart limit used with crash-loop protection. |
| `restart_window` | duration | `5m` | Restart failure counting window. |
| `stable_after` | duration | `1m` | Stable runtime after which the restart counter is cleared. |
| `run_as` | mapping | current user | Optional `user` and `group`; validated and applied by the host process adapter. |
| `resources` | mapping | not configured | Optional process, memory, and CPU policy. Status is explicit when the host cannot enforce it. |
| `healthcheck` | mapping | disabled | Optional command or native-probe health monitoring. |
| `depends_on` | mapping | none | Optional service dependencies. |

Command resolution follows two rules:

1. If `command` contains `/` or `\\`, a relative path is resolved from the
   service working directory.
2. Otherwise, Mango searches the host `PATH`.

Shell syntax is not parsed. To use a shell, make the shell the command:

```yaml
services:
  report:
    command: sh
    args: [-c, "generate-report | gzip > report.gz"]
```

### `healthcheck`

Use one direct probe with `test`:

```yaml
healthcheck:
  test: [CMD, curl, -f, http://127.0.0.1:8080/health]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 30s
  start_interval: 5s
```

Or define multiple probes with `checks`:

```yaml
healthcheck:
  policy: all
  checks:
    - test: [CMD, curl, -f, http://127.0.0.1:8080/]
    - test: [CMD, curl, -f, http://127.0.0.1:9090/metrics]
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `test` | sequence of strings | — | One probe. The first item must be `CMD`, `CMD-SHELL`, `HTTP`, `HTTPS`, `TCP`, `FILE`, or `NONE`. |
| `policy` | string | `all` | With multiple checks, `all` requires every check to pass; `any` requires one. |
| `on_unhealthy` | string | `report` | `report` records the result; `restart` or `stop` performs the configured lifecycle action. |
| `cooldown` | duration | `30s` | Minimum time between automatic health actions. |
| `checks` | sequence of mappings | — | Multiple probes. Cannot be combined with `test`. |
| `interval` | duration | `10s` | Normal interval between probe rounds. |
| `timeout` | duration | `5s` | Maximum time allowed for one probe. |
| `retries` | integer | `3` | Consecutive failures required to mark a probe unhealthy. |
| `start_period` | non-negative duration | `0s` | Startup grace period during which failures do not increase the failing streak. |
| `start_interval` | duration | `interval` | Probe interval during startup. |

Probe forms:

- `CMD` runs the first command item directly and passes the remaining items as
  arguments: `[CMD, curl, -f, URL]`.
- `CMD-SHELL` joins the remaining items into a shell command. It uses `/bin/sh`
  on Unix-like systems and `cmd /C` on Windows.
- `HTTP` and `HTTPS` perform a native GET and require a 2xx response: `[HTTP,
  http://127.0.0.1:8080/health]`.
- `TCP` opens a native TCP connection: `[TCP, 127.0.0.1:8080]`.
- `FILE` succeeds when the target file exists: `[FILE, ready.flag]`.
- `test: [NONE]` disables the health check. `NONE` is not allowed inside
  `checks`.

Probes run on the host with the service's working directory and environment.
Health is reported independently from lifecycle state; an unhealthy result
does not by itself restart the service unless `on_unhealthy` requests it. Any
automatic restart uses the service's normal backoff and crash-loop budget. A
single `test` is shown as the `default` check; entries in `checks` are named
`check-1`, `check-2`, and so on.

### `depends_on`

```yaml
services:
  web:
    command: ./bin/web
    depends_on:
      db:
        condition: service_healthy
        restart: true
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `<service-name>` | mapping key | — | Must refer to another service in the same project. |
| `condition` | string | `service_started` | `service_started`, `service_healthy`, or `service_completed_successfully`. |
| `restart` | boolean | `false` | Restart this dependent when the dependency is explicitly restarted or recreated by an apply. |

When a dependency condition is not met, the dependent stays in `waiting` and
does not receive a PID. `service_healthy` requires the dependency to have an
enabled health check. `service_completed_successfully` requires the dependency
to use `restart: never`. Unknown dependencies and dependency cycles are
rejected during validation.

### `tasks`

Tasks are one-off commands and do not have service lifecycle state:

```yaml
tasks:
  nightly-job:
    command: ./bin/backup
    args: [--database, app]
    working_dir: .
    environment:
      APP_ENV: production
    timeout: 30m
    concurrency: forbid
    retry:
      retries: 3
      delay: 10s
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `<task-name>` | mapping key | — | Unique task name within the project. |
| `command` | string | — | Required executable name or path. It follows the same direct-execution rules as services. |
| `args` | sequence of strings | `[]` | Arguments passed to `command`. |
| `working_dir` | string | `defaults.working_dir` or `.` | Working directory for the task. |
| `environment` | string-or-reference map | inherited environment | Variables to add or override; `from_env` and `from_file` references resolve per attempt. |
| `timeout` | duration | no limit | Starts after the process starts. A timeout force-stops the process tree and records exit code `124`. |
| `concurrency` | string | `forbid` | `forbid` queues overlapping invocations; `allow` runs them in parallel. |
| `retry` | mapping | no retries | Retry settings for failed or timed-out attempts. |
| `outputs` | sequence of relative paths | `[]` | Files to inspect after the task; history records existence, size, and SHA-256 metadata. |

`retry` fields:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `retries` | non-negative integer | `0` | Number of retries after the initial attempt. Total attempts are `retries + 1`. |
| `delay` | non-negative duration | `0` | Wait between failed attempts. |

Task and workflow execution logs are separate from service logs. A direct task
named `cleanup` writes to `PROJECT/task/cleanup`; a workflow node writes to
`PROJECT/workflow/WORKFLOW/NODE`.

### `workflows`

Workflows are DAGs made from task nodes:

```yaml
workflows:
  release:
    concurrency: forbid
    steps:
      build:
        task: build
      test:
        task: test
        needs: [build]
        timeout: 10m
        retry:
          retries: 2
          delay: 5s
      publish:
        task: publish
        needs: [test]
        allow_failure: false
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `<workflow-name>` | mapping key | — | Unique workflow name within the project. |
| `concurrency` | string | `forbid` | `forbid` skips a new run while the same workflow is active; `allow` permits overlap. |
| `steps` | mapping | — | At least one workflow node. |
| `<node-name>` | mapping key | — | Unique node name within the workflow. |
| `task` | string | — | Required name of a task in the same project. |
| `needs` | sequence of strings | `[]` | Other node names that must finish successfully before this node runs. |
| `timeout` | duration | task setting | Optional node-level timeout override. |
| `retry` | mapping | task setting | Optional node-level retry override. |
| `allow_failure` | boolean | `false` | Lets downstream nodes continue and does not fail the workflow when this node fails. |

Independent nodes can run in parallel. If a required node fails, its descendants
are skipped with an `upstream_failed` reason while independent branches
continue. Workflow and task definitions are validated for unknown references,
duplicate dependencies, self-dependencies, and cycles.

### `schedules`

Schedules trigger an existing task or workflow; they do not define a command:

```yaml
schedules:
  - name: nightly-release
    cron: "0 2 * * *"
    timezone: Asia/Taipei
    run: workflow/release
    misfire: catch_up
    max_catch_up: 2

  - name: hourly-cleanup
    cron: "0 * * * *"
    run: task/cleanup
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | yes | Unique schedule name within the project. |
| `cron` | string | yes | Five-field standard cron expression. |
| `timezone` | string | no | IANA time zone, such as `UTC` or `Asia/Taipei`; defaults to local time. |
| `run` | string | yes | `task/<task-name>` or `workflow/<workflow-name>`. |
| `misfire` | string | no | `skip`, `run_once`, or `catch_up`; defaults to `skip`. |
| `max_catch_up` | non-negative integer | no | Maximum missed activations to run for `catch_up`; capped at `1000`. |

Schedules use durable occurrence IDs and idempotency keys, so a daemon restart
cannot create a duplicate logical run. `skip` (the default) records missed
slots without backfilling them, `run_once` runs the latest missed slot once,
and `catch_up` runs at most `max_catch_up` missed slots. A nonexistent local
time during spring-forward is skipped; both UTC instants of an ambiguous
fall-back time are distinct occurrences. In v4, schedule-level `command`,
`args`, `working_dir`, `timeout`, `retry`, and `concurrency` fields are not
supported; put execution settings on the target task instead.

### `webhooks`

Webhooks optionally trigger an existing task or workflow through an authenticated
loopback HTTP listener:

```yaml
webhooks:
  - name: release-hook
    path: /hooks/release
    target_type: workflow
    target: release
    secret_ref: env:MANGO_WEBHOOK_SECRET
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | yes | Unique webhook name within the project. |
| `path` | string | yes | Unique absolute listener path; query strings and fragments are rejected. |
| `target_type` | string | yes | `task` or `workflow`. |
| `target` | string | yes | Existing task or workflow name in the same project. |
| `secret_ref` | string | yes | Currently `env:NAME`; the secret is never stored in execution history. |

The listener is disabled by default. Requests must be `POST`, include an
`Idempotency-Key`, a recent `X-Mango-Timestamp`, and an HMAC-SHA256
`X-Mango-Signature` over `timestamp + "." + raw_body`. Duplicate deliveries
reuse the original run; reusing a key with a different body returns `409`.

### Value formats

Durations use Go duration syntax, for example `500ms`, `10s`, `5m`, or `1h`.
Most duration fields must be positive when present. `start_period` and retry
delays may be zero.

Sizes are plain positive bytes or use `KB`, `MB`, `GB`, `KiB`, `MiB`, or `GiB`:

```yaml
defaults:
  log_max_size: 100MiB
```

## Complete CLI reference

### Command-line conventions

Notation used below:

- `<value>` is required.
- `[value]` is optional.
- `...` accepts one or more additional values.
- `PROJECT/NAME` means a project-qualified resource, such as
  `demo/api` or `demo/nightly`.

Project names must start with an ASCII letter and may then contain letters,
digits, `.`, `_`, and `-`. Service, task, workflow, schedule, and workflow-node
names may also start with a digit.

Most commands require the daemon. If it is unavailable, Mango reports:
`daemon is not running; start it with: mango daemon start`.

### Global options

Global options may appear before or after the command:

| Option | Values / default | Description |
| --- | --- | --- |
| `--color` | `auto` (default), `always`, `never` | Controls ANSI colors. `auto` enables colors for interactive terminals and disables them for pipes/CI. |
| `--json` | off by default | Prints structured JSON where the command supports it. JSON output never contains ANSI color codes. |
| `--no-trunc` | off by default | Shows full execution IDs in human-readable output; JSON already uses full IDs. |

Both forms are accepted:

```sh
mango --color=never ls
mango status demo/api --json
mango --json history ls --limit 20
mango --no-trunc execution ls
```

Set `NO_COLOR` to disable colors in `auto` mode. `--color=always` overrides
that setting. `--json` is supported by daemon status, project listing/apply,
service inspection and lifecycle commands, task/workflow/schedule queries,
startup status, and `doctor`. It is not supported by interactive monitor/log
commands, configuration initialization/validation, daemon
start/stop/restart/logs, project add/remove/rename, or startup
install/uninstall.

`--` ends global-option parsing and passes remaining values as command
arguments.

### `mango init`

```text
mango init [PATH] [--force]
```

`init` writes the complete embedded `mango.example.yaml` to `PATH` (default
`./mango.yaml`) with mode `0600`, creates missing parent directories, and
copies the Go sources used by the configuration into `PATH`'s adjacent
`examples/` directory. It supports only `.yaml` paths and never registers or
applies a project. An existing YAML causes an error unless `--force` is
supplied. Without `--force`, existing example sources are preserved and
missing sources are added; with `--force`, matching embedded sources are
updated. Extra files in `examples/` are never removed. `--json` is not
supported.

### `mangod`

```text
mangod run
```

`mangod` has one entry point. `run` starts the daemon in the foreground; press
Ctrl+C to stop it. The daemon owns service processes, schedules, task/workflow
execution, local IPC, logs, and execution history. Only one `mangod run` can
own a given `MANGO_HOME`; a second direct invocation exits with
`daemon is already running`.

### `mango daemon`

```text
mango daemon start
mango daemon stop
mango daemon restart
mango daemon status [--json]
mango daemon logs [--tail N] [--follow]
```

| Command | Arguments / flags | Description |
| --- | --- | --- |
| `start` | none | Starts `mangod` in the background and waits for its health endpoint. It is idempotent: concurrent callers succeed while only one daemon runs. `mangod` must be next to `mango` or on `PATH`. |
| `stop` | none | Requests a graceful daemon shutdown. |
| `restart` | none | Stops the daemon, waits for the IPC endpoint to close, then starts it again. |
| `status` | `--json` | Shows daemon status, PID, API version, and project configuration errors. A stopped daemon is reported as `stopped`. |
| `logs` | `--tail N`, `--follow` | Reads the local daemon log without requiring a running daemon. |

`daemon logs` defaults to the last 15 lines. `--tail 0` prints the complete
daemon log. `--follow` prints the initial tail and then waits for new lines;
press Ctrl+C to stop following.

If a registered project cannot be loaded, the daemon continues serving other
projects and reports `degraded` status. Fix the YAML and run
`mango project apply PROJECT` to recover it.

Examples:

```sh
mango daemon start
mango daemon status
mango daemon status --json
mango daemon logs --tail 50
mango daemon logs --follow
mango daemon restart
mango daemon stop
```

### `mango project`

```text
mango project add NAME PATH
mango project remove NAME
mango project rename OLD NEW
mango project plan PROJECT [--json]
mango project status PROJECT [--json]
mango project apply NAME [--wait] [--json]
mango project rollback PROJECT [GENERATION] [--wait] [--json]
mango project ls [--json]
```

| Command | Parameters | Description |
| --- | --- | --- |
| `add` | `NAME`, `PATH` | Validates and registers a `.yaml` file. The stored path is absolute. Duplicate names are rejected. |
| `remove` | `NAME` | Removes the project from the registry. It does not delete the YAML file, application files, or logs. A running daemon unloads the project's services and execution definitions. |
| `rename` | `OLD`, `NEW` | Re-registers the existing YAML path under a new name. It does not edit the YAML file. |
| `plan` | `PROJECT` | Reads the current YAML and shows the deterministic plan v2 resources for services, tasks, workflows, and schedules without changing runtime or metadata state. |
| `status` | `PROJECT` | Shows the accepted generation, reconciliation phase, readiness, last project error, and resource status. |
| `apply` | `NAME`, optional `--wait` | Validates and compiles the YAML, stores an immutable accepted generation when the effective configuration changes, and starts background reconciliation. Unchanged effective configuration reuses the current generation. Runtime failures are shown by `status`; `--wait` waits until ready. |
| `rollback` | `PROJECT`, optional `GENERATION`, `--wait` | Accepts an earlier accepted snapshot as a new generation and starts background reconciliation. `--wait` waits until ready. |
| `ls` | none | Lists registered projects, enabled status, YAML path, config version, and last accepted time. Requires the daemon. |

Plan v2 JSON has `plan_version: 2`, a current generation, an advisory
`proposed_generation`, and one sorted `resources` array. Each resource has a
`kind`, `name`, `action`, `pending`, `process_affecting`, and deterministic
fingerprints; dependency restarts include a stable sorted reason. Applying a
project validates and compiles the YAML, writes a committed immutable
snapshot, then updates the registry pointer before waking reconciliation. The
registry therefore represents the latest accepted desired state, not the last
runtime-successful generation. Runtime failures are retried in the background
and exposed through `project status`; an old `apply-operations.json`, if
present, is ignored and is not removed automatically. Committed v1 generation
snapshots remain readable.

Examples:

```sh
mango config validate ./mango.yaml
mango project add demo ./mango.yaml
mango project ls
mango project apply demo
mango project rename demo staging
mango project remove staging
```

`project add`, `remove`, and `rename` update the local registry even if the
daemon is not running; they attempt to notify a running daemon to reload.

### `mango config validate`

```text
mango config validate PATH
```

`PATH` is required and must point to a `.yaml` file. This command parses and
validates the complete schema without starting or stopping anything. It checks
version, names, required commands, duration/size formats, restart/concurrency
values, health checks, dependencies, workflow DAGs, cron expressions, time
zones, and schedule targets.

```sh
mango config validate mango.example.yaml
```

### Service inspection

#### `mango ls`

```text
mango ls [--json]
```

Lists every managed service. The text table contains:

| Column | Meaning |
| --- | --- |
| `ID` | Runtime service ID. IDs are reassigned from zero when the daemon starts. |
| `SERVICE` | Stable `PROJECT/SERVICE` key. |
| `SUPERVISOR` | Process supervisor: `legacy` or `shim`. |
| `PROCESS` | Root executable name; child processes are shown as indented rows. |
| `STATE` | Mango lifecycle state. |
| `HEALTH` | Health-check result, or `-` when no check is configured. |
| `OS STATE` | Operating-system process state. |
| `PID` | Root or child process ID. |
| `PORTS` | Listening TCP and bound UDP ports. |
| `CPU%` / `RSS` / `MEM%` | Best-effort process resource metrics. |
| `RESTART` | Automatic restart count for managed services. |

Child processes are informational only and cannot be controlled individually.
The parent service aggregates listening ports from its process tree where the
operating system permits inspection; child CPU and memory values are shown in
their own rows.

```sh
mango ls
mango ls --json > services.json
```

#### `mango status`

```text
mango status PROJECT/SERVICE|ID [--json]
```

The single target may be a stable service key such as `demo/api` or a
non-negative runtime ID such as `2`. The detailed view includes state, health,
OS state, PID, ports, start time, uptime, CPU, RSS, memory percentage, restart
count, last exit code, disabled state, command line, stdout/stderr log paths,
last error, and unsatisfied dependencies.

```sh
mango status demo/api
mango status 2 --json
mango status demo/api --watch
```

`--watch` refreshes the service status until interrupted. Resource policies are
shown in JSON under `resources`, including per-limit capability state and an
overall `supported`, `degraded`, or `unsupported` result.

### `mango events`

```text
mango events [--limit N] [--follow] [--json]
```

Reads the durable event stream covering administrative operations, execution
transitions, service state, configuration, schedule, webhook, and audit
activity. `--follow` polls after the last event ID and exits cleanly on Ctrl-C.
Event metadata contains only safe fields; secret values are never included.
Human-readable event rows use the same short-ID display rule. The API event
filter keeps `run_id` exact because events may reference workflow child task
IDs rather than root executions.

### Service lifecycle commands

```text
mango start   TARGET [TARGET ...] [--json]
mango stop    TARGET [TARGET ...] [--json]
mango restart TARGET [TARGET ...] [--json]
mango enable  TARGET [TARGET ...] [--json]
mango disable TARGET [TARGET ...] [--json]
```

`TARGET` can be:

- `PROJECT`, which selects every service in that project.
- `PROJECT/SERVICE`, which selects one service.
- A non-negative numeric service `ID`.

Multiple targets are accepted:

```sh
mango start demo
mango stop demo/api 2 other/web
mango restart demo/api
mango disable demo/api
mango enable demo/api
```

| Command | Behavior |
| --- | --- |
| `start` | Starts the service and clears its disabled/crash-loop state. If dependencies are not ready, it enters `waiting`. |
| `stop` | Stops the service without changing its YAML `autostart` value. |
| `restart` | Stops and starts the service. A single-service restart also propagates to dependents whose dependency entry has `restart: true`. |
| `enable` | Clears the disabled state and starts the service. |
| `disable` | Marks the service disabled and stops it. It remains disabled until `enable` or an apply that recreates the service from a changed definition. |

For a project target, `start`/`enable` use dependency-first order and
`stop`/`disable` use dependent-first order. A project `restart` stops in reverse
order and starts in dependency-first order. Bulk operations continue after an
individual failure and return a non-zero exit status if any target failed.
These commands have no command-specific flags; `--follow` belongs to `logs`.

Possible lifecycle states are `stopped`, `starting`, `waiting`, `running`,
`stopping`, `exited`, `backing_off`, `crash_loop`, `failed`, `disabled`, and
`unknown`.

Restart policies behave as follows:

- `never`: keep the service in `exited` after it stops.
- `on-failure`: restart only after a non-zero exit or another abnormal exit.
- `always`: restart after any exit, including a clean exit.

Automatic restarts use exponential backoff from about one second up to one
minute. When the restart limit is exceeded, the service enters `crash_loop`.
`start` or `restart` can be used to try again.

### `mango logs`

Read service, task, or workflow-node output:

```text
mango logs TARGET [TARGET ...] [--stream STREAM] [--tail N] [--follow]
mango logs clear TARGET
```

Log targets are:

| Target form | Example | Resolves to |
| --- | --- | --- |
| `PROJECT/SERVICE` | `demo/api` | Service stdout/stderr logs. |
| service ID | `2` | A service found by runtime ID. |
| `PROJECT/task/TASK` | `demo/task/cleanup` | Direct task execution logs. |
| `PROJECT/workflow/WORKFLOW/NODE` | `demo/workflow/release/build` | One workflow node's logs. |

Flags:

| Flag | Default | Description |
| --- | --- | --- |
| `--stream` | `all` | Select `stdout`, `stderr`, or `all`. |
| `--tail` | `15` | Number of lines to read initially. |
| `--follow` | off | Continue reading new output until Ctrl+C. |

Examples:

```sh
mango logs demo/api
mango logs demo/api --stream stdout --tail 50
mango logs demo/api --stream stderr
mango logs demo/api --follow
mango logs demo/api demo/task/cleanup --stream all --tail 20
mango logs clear demo/api
```

When multiple targets or streams are requested, each line is prefixed with its
canonical target. `stdout` and `stderr` are color-coded in interactive output.
`logs clear` truncates current stdout/stderr files and removes their numeric
rotation files; a running service can continue writing after the clear.

Service log files use the service-level log rotation settings, with runtime
defaults when they are omitted. `logs` does not support `--json`.

### `mango monitor`

```text
mango monitor
```

With a TTY, `monitor` redraws the service table every second and allows actions
on the selected service. Without a TTY it prints one service table and exits.
It does not support `--json`.

| Key | Action |
| --- | --- |
| `Up` / `Down` or `k` / `j` | Select the previous/next service. |
| `s` | Stop selected service. |
| `r` | Restart selected service. |
| `e` | Enable selected service. |
| `d` | Disable selected service. |
| `l` | Follow selected service logs; press any key to return. |
| `Enter` | Show selected service details; press any key to return. |
| `q` or Ctrl+C | Quit. |

### Tasks

```text
mango task ls [--json]
mango task run PROJECT/TASK [--json]
mango execution ls [--status STATUS | --all] [--trigger-type TYPE] [--trigger NAME]
                       [--project PROJECT] [--target-type task|workflow]
                       [--target PROJECT/NAME] [--limit N] [--json]
mango execution get|cancel|retry RUN_REF [--json]
mango execution watch RUN_REF [--timeout 45s] [--json]
mango execution logs RUN_REF [--stream stdout|stderr|all] [--tail N] [--json]
```

| Command / flag | Description |
| --- | --- |
| `task ls` | Lists task name, lifetime logical runs, status, last run, next run, and duration. |
| `task run` | Starts a task asynchronously with trigger `manual`; the command returns after the run is accepted. |
| `execution ls` | Lists queued/running executions by default; `--status` selects one status and `--all` includes every status. |
`task ls` includes `RUNS`, `STATUS`, `LAST RUN`, `NEXT RUN`, and `DURATION`.
Without `--json`, `task run` prints the accepted state and an operator-friendly
run reference, for example `Task demo/backup queued (run_id=...)`. JSON keeps
the canonical durable `run_id`.

The example configuration also includes a long-running execution target for
testing execution controls:

```sh
mango task run demo/execution-demo-task
mango workflow run demo/execution-demo-workflow
```

Both targets run for about 20 seconds and emit both stdout and stderr. Use the
returned run reference with `execution watch`, `execution cancel`,
`execution retry`, `execution logs`, or `execution ls`.
`RUNS` counts direct task runs and workflow-node invocations; retries do not
increase it. `NEXT RUN` includes the earliest direct or indirect schedule.

### Workflows

```text
mango workflow ls [--json]
mango workflow run PROJECT/WORKFLOW [--json]
```

| Command / flag | Description |
| --- | --- |
| `workflow ls` | Lists workflow name, lifetime logical runs, status, last run, next run, and duration. |
| `workflow run` | Starts a workflow asynchronously with trigger `manual`. |
`workflow ls` includes `RUNS`, `STATUS`, `LAST RUN`, `NEXT RUN`, and
`DURATION`. `RUNS` counts workflow root invocations and `NEXT RUN` is the
earliest direct schedule for the workflow.
Without `--json`, `workflow run` prints the accepted state and an
operator-friendly run reference, for example
`Workflow demo/release queued (run_id=...)`. JSON keeps the canonical durable
`run_id`.

Examples:

```sh
mango workflow ls
mango workflow run demo/release
```

Use `mango history ls --target-type workflow --target demo/release` to inspect
workflow runs and their task nodes.

Every task and workflow run returns a durable `run_id`. Use `execution watch`
or `execution get` to observe its final status, `execution cancel` to terminate
the managed process tree, `execution retry` to create a new logical execution,
and `execution logs` to read output after the process exits.
`execution watch` waits up to 30 seconds by default; use `--timeout 2m` for a
longer wait (up to 5 minutes).

Execution commands accept either the full `run_id` or a unique prefix of at
least 8 characters. Exact IDs are checked first; an ambiguous prefix fails
without performing the requested operation and returns usable candidate refs.
Human-readable output shows generated UUIDs as 12 compact hexadecimal
characters. Use the global `--no-trunc` flag to show full IDs. JSON output
always keeps the canonical full `run_id`.

For example:

```sh
mango execution watch 7f31a2c4d9e0
mango execution get 7f31a2c4d9e0 --no-trunc
```

`execution ls` supports the following filters:

```sh
mango execution ls
mango execution ls --status running
mango execution ls --status failed
mango execution ls --all
mango execution ls --trigger-type schedule --trigger nightly
mango execution ls --project demo --target-type task
mango execution ls --target demo/execution-demo-task --limit 10
mango execution ls --json
```

By default `execution ls` lists only `queued` and `running` executions. Use
`--status STATUS` for one status or `--all` for every status; `--all` and
`--status` are mutually exclusive. The human-readable table shows only
`RUN_ID`, target, status, started, and elapsed. `--target` accepts either
`PROJECT/NAME` or a name used together with `--project`; `--limit 0` returns
all matching executions. `--trigger-type` accepts `schedule`, `webhook`, or
`manual`; `--trigger` filters the trigger name.

### Schedules

```text
mango schedule ls [--json]
mango schedule enable PROJECT|PROJECT/SCHEDULE [PROJECT|PROJECT/SCHEDULE ...] [--json]
mango schedule disable PROJECT|PROJECT/SCHEDULE [PROJECT|PROJECT/SCHEDULE ...] [--json]
```

| Command / flag | Description |
| --- | --- |
| `schedule ls` | Lists schedule, trigger type, target, cron expression, time zone, lifetime runs, status, last run, next run, and duration. |
| `schedule enable TARGET ...` | Enables future cron triggers for one or more schedules. A `PROJECT` target enables every schedule in that project. |
| `schedule disable TARGET ...` | Disables future cron triggers for one or more schedules. A `PROJECT` target disables every schedule in that project. Existing task/workflow runs are not interrupted. |

Examples:

```sh
mango schedule ls
mango schedule disable demo/nightly-release
mango schedule enable demo/nightly-release
mango schedule disable demo
```

Schedule enable/disable state is stored in `MANGO_HOME/state/schedules.json`
and survives daemon restarts. A project apply keeps state for unchanged
`PROJECT/SCHEDULE` keys, enables new schedules by default, and removes state
for schedules removed from the configuration. Disabled schedules remain in
`schedule ls` with `STATUS=disabled` and no `NEXT_RUN`.

### `mango history`

```text
mango history ls [--limit N] [--status STATUS] [--trigger-type TYPE]
                 [--trigger NAME] [--project PROJECT]
                 [--target-type task|workflow] [--target PROJECT/NAME]
                 [--attempts] [--json]
mango history show RUN_REF [--json]
mango history purge (--before RFC3339 | --all) --yes [--json]
```

History is terminal-only and is read from the canonical execution store.
`history ls` is newest-first and `--limit 0` returns all retained terminal
runs. `history show` accepts terminal runs and returns workflow nodes, task
records, attempts, and lifecycle events. `history purge` removes only terminal
runs and their tasks, attempts, and events; it never removes active metadata,
lifetime counters, or log files. Purged run IDs return not found.

History JSON uses stable snake_case `HistoryInfo` fields. Text output shows
only `RUN_ID`, target, status, started, and elapsed; `--attempts` includes task
and attempt detail in JSON. In an
interactive terminal, the shared browser navigates `run → task → attempts` or
`run → workflow → tasks → attempts`; each history table shows 15 rows per page.
Runs are ordered newest-first, while tasks and attempts are ordered oldest-first.
Use `←`/`→` (or PageUp/PageDown) to change pages, `↑`/`↓` to select within the
current page, `Esc` to go back, `r` to refresh, and `q` to quit.
Nested task run IDs are displayed with the same shortening rule for readability;
execution commands resolve root execution IDs only.

### Startup integration

```text
mango startup install
mango startup uninstall
mango startup status [--json]
```

| Command | Description |
| --- | --- |
| `install` | Installs per-user daemon startup integration that launches at boot, before interactive login. It may request one-time administrator authorization. |
| `uninstall` | Removes the integration. It does not remove project files or logs. |
| `status` | Reports whether integration is installed, boot-enabled, and where it is configured. |

The platform mechanisms are a Windows Task Scheduler boot trigger with an S4U
principal, Linux `systemd --user` with user lingering, and a macOS `launchd`
LaunchDaemon with `UserName` set to the current user. They all start the
user-owned `mangod` before interactive login. When `MANGO_HOME` is set, each
definition passes the same value to `mangod`. Linux restarts only after failure,
macOS keeps the job alive only after an unsuccessful exit, and Windows uses
`MultipleInstancesPolicy=IgnoreNew`. Windows S4U startup is deliberately local:
it cannot use network credentials or Windows encrypted-file access. The Linux
uninstall command removes Mango's unit but leaves user lingering enabled because
that setting may be shared by other user services. The daemon lock remains the
final guard for direct or overlapping launches. Startup integration launches
only `mangod`; service startup still depends on each service's `autostart`
setting. The daemon binary must be available next to `mango` or on `PATH` when
installing.

```sh
mango startup install
mango startup status
mango startup uninstall
```

### `mango doctor`

```text
mango doctor [--json]
```

Checks and prints grouped environment, database, and daemon diagnostics,
including the `daemon.yaml` and `state/schedules.json` paths. The database
connection and history schema statuses are based on the connection currently
used by the daemon. It also shows safe connection metadata such as connection
type, host, database, login, and port; passwords and extra DSN options are
never returned. If the daemon is unavailable, those statuses are `unknown`;
configured paths alone are not treated as proof that the Mango service can
read or write history. It is useful when a daemon cannot start or a project is
missing from the service list.

```sh
mango doctor
mango doctor --json
```

### Help and exit status

```text
mango help
mango -h
mango --help
```

Successful commands normally exit with `0`. Invalid arguments, validation
errors, daemon connection failures, and failed operations exit non-zero. Bulk
service operations may perform successful targets while still returning a
non-zero status if another target fails.

## Repository examples

The repository includes small programs that are useful for testing services,
tasks, logs, retries, and timeouts:

### API service

```text
go run ./examples/api/main.go [--port PORT] [--interval DURATION]
go run ./examples/api/main.go --supervise --port PORT --port PORT [--interval DURATION]
```

| Option | Default | Description |
| --- | --- | --- |
| `--port` | `8080` | HTTP listening port from `1` to `65535`; may be repeated. In supervisor mode, each port is served by a separate child process. |
| `--interval` | `5s` | Heartbeat interval. |
| `--supervise` | off | Starts one independent `go run` API child per specified port. |

The server provides `GET /` for a successful response and `GET /error` for an
intentional HTTP 500 plus stderr output.

```sh
go run ./examples/api/main.go --port 8080 --interval 3s
go run ./examples/api/main.go --supervise --port 9000 --port 9090 --interval 3s
curl http://127.0.0.1:8080/
curl http://127.0.0.1:8080/error
```

The supervisor mode is used by `api-supervisor` in `mango.example.yaml`. It preserves the
process-tree demonstration: Mango starts one Go supervisor, which starts two
independent Go API processes listening on ports `9000` and `9090`. This stays
cross-platform because all levels are implemented with Go and `go run`.

### One-off task

```text
go run ./examples/one-task/main.go [--iterations N] [--interval DURATION] [--fail]
```

| Option | Default | Description |
| --- | --- | --- |
| `--iterations` | `5` | Number of iterations; must be at least `1`. |
| `--interval` | `500ms` | Delay between iterations. |
| `--fail` | off | Exit with code `2` after writing output. |

Example:

```sh
go run ./examples/one-task/main.go --iterations 3 --interval 200ms --fail
```

### Workflow task helpers

`examples/tasks/emit.go` is the cross-platform task helper used by the sample.
It accepts:

```text
go run ./examples/tasks/emit.go [--name NAME] [--steps N] [--delay DURATION] [--fail]
```

| Option | Default | Description |
| --- | --- | --- |
| `--name` | `go-task` | Name written to the task log. |
| `--steps` | `3` | Number of steps; must be positive. |
| `--delay` | `250ms` | Delay between steps using Go duration syntax. |
| `--fail` | off | Write a failure message and exit with code `7`. |

The sample uses this helper for normal tasks, intentional failures, retries,
parallel branches, and timeout demonstrations.

## Runtime files

By default, Mango uses the operating system's per-user configuration and cache
directories. Set `MANGO_HOME` to isolate a development or test environment:

```sh
MANGO_HOME=/tmp/mango-test ./bin/mango doctor
```

PowerShell:

```powershell
$env:MANGO_HOME = "C:\temp\mango-test"
```

An isolated home contains the registry, optional daemon configuration, runtime
files, logs, and execution history:

```text
MANGO_HOME/
├── projects.json
├── daemon.yaml                 # optional daemon settings
├── daemon.log
├── runtime/
│   ├── daemon.lock              # persistent file; lock is released on close
│   ├── daemon.pid
│   ├── mango.sock              # Unix; Windows uses a named pipe
│   └── shims/
│       └── <service-key-hash>/
│           └── <incarnation>/
│               ├── bootstrap.json
│               ├── status.json
│               ├── exit.json
│               ├── endpoint
│               ├── shim.pid
│               └── instance.lock
├── logs/
└── state/
    ├── history.db
    ├── history.db.bak        # first compatible SQLite backup; never overwritten
    ├── history.db.migration.lock
    ├── history.db.migration.json
    ├── history.db.migration-<run-id>.bak  # backup for an actual schema upgrade
    ├── schedules.json       # persisted schedule enable/disable state
    ├── apply-operations.json     # legacy file, ignored if present
    └── generations/
        └── <project>/<generation>.json  # last and previous desired states
```

The SQLite metadata database also contains durable `events` and
`audit_entries` records. Secret references and resource policies are stored as
provider/policy metadata only; resolved secret values are never persisted.

The optional `daemon.yaml` supports execution-history and event retention,
database, webhook listener, and management HTTP API configuration. SQLite is
used by default. The metadata schema is maintained by explicit, named,
checksum-verified migrations. A fresh database does not need a backup; an
actual SQLite upgrade creates a run-specific backup and preserves the first
`history.db.bak` compatibility backup when it does not already exist.

```yaml
# MANGO_HOME/daemon.yaml
schedule_history_limit: 1000
event_retention_limit: 10000

history:
  database:
    driver: sqlite
    # Relative paths are resolved from MANGO_HOME.
    path: state/history.db

webhook_server:
  enabled: false
  listen: 127.0.0.1:8787
  max_body_bytes: 1048576
  replay_window: 5m
  rate_limit_per_minute: 60

http_server:
  enabled: false
  listen: 127.0.0.1:8788
  # Required for non-loopback listeners; resolved at daemon start.
  token_ref: env:MANGO_HTTP_TOKEN
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `schedule_history_limit` | non-negative integer | `0` | Maximum number of terminal execution records to retain; active executions are never pruned. `0` keeps all records. |
| `event_retention_limit` | non-negative integer | `0` | Maximum number of durable observability events to retain. `0` keeps all events and is independent of execution-history retention. |
| `webhook_server.enabled` | boolean | `false` | Enables the optional authenticated webhook listener. |
| `webhook_server.listen` | address | `127.0.0.1:8787` | Listener address; loopback is the safe default. |
| `webhook_server.max_body_bytes` | positive integer | `1048576` | Maximum accepted raw request body size. |
| `webhook_server.replay_window` | duration | `5m` | Allowed timestamp age/skew for signed requests. |
| `webhook_server.rate_limit_per_minute` | positive integer | `60` | Per-client and per-path request limit. |
| `http_server.enabled` | boolean | `false` | Enables the versioned management API. |
| `http_server.listen` | address | `127.0.0.1:8788` | Management listener; loopback is the safe default. |
| `http_server.token_ref` | `env:NAME` or `file:PATH` | none | Bearer token reference. Required for non-loopback listeners. The token is resolved at daemon start and never logged. |
| `http_server.require_auth` | boolean | `false` | Requires bearer authentication even on loopback. |

Supported database drivers are `sqlite`, `postgres`, and `mysql`. PostgreSQL
and MySQL require a DSN, preferably supplied through an environment variable:

```yaml
history:
  database:
    driver: postgres
    dsn_env: MANGO_HISTORY_DATABASE_DSN
```

For SQLite, omitting `path` uses `MANGO_HOME/state/history.db`. Existing
`state/execution-history.json` files are preserved as backups but are not
imported or read.

If a metadata migration fails, do not delete or edit the database. Keep the
daemon stopped and run `mango doctor --json`. The migration marker identifies
the verified backup and the last error. A consistent `running` or `failed`
marker can be retried; a checksum mismatch or inconsistent marker is blocked
until the operator restores the marker's backup. PostgreSQL and MySQL require
an operator-managed backup before migration; Mango never performs an
automatic schema downgrade or rollback.

## Platform behavior

- Legacy services are still owned by `mangod` and use the existing process
  backend.
- Shim services are owned by one `mango-shim` process per service. The shim
  writes service stdout/stderr directly to the configured log files, so daemon
  failure does not interrupt service logging.
- On Linux, the shim uses a dedicated process group and a child subreaper to
  collect adopted descendants. On macOS it uses a dedicated process group;
  descendants that deliberately escape the group are outside the guarantee.
- On Windows, the shim assigns the service to a dedicated Job Object and uses
  job termination for process-tree containment.
- An unexpected `mangod` exit leaves shim services running. A new daemon scans
  `runtime/shims`, validates the service key, incarnation, fingerprint, and
  process identity, then attaches to a matching shim without starting a
  duplicate service.
- `mango daemon stop` and explicit SIGTERM/SIGINT shutdowns stop shim services
  first. Arbitrary daemonization, `setsid`, namespace escape, and Windows
  breakaway processes are not covered by the v1 containment guarantee.
- `mango startup install` uses a Windows Task Scheduler boot trigger with S4U,
  Linux `systemd --user` plus user lingering, or a macOS `launchd` LaunchDaemon.

### Capability reporting and local IPC

`mango doctor` and the daemon `health` response expose a `capabilities` map.
Each capability has one of three states: `supported`, `degraded`, or
`unsupported`, plus a detail string explaining the platform boundary. The
report covers process-tree termination, graceful signals, user/group
execution, CPU/memory limit primitives, startup integration, health probes,
runtime-state persistence, and local IPC. Windows reports Job Object process
containment and named-pipe IPC; Linux reports process groups, the shim
subreaper path, and cgroup availability; macOS reports the process-group
escape limitation and the absence of a portable Mango resource-limit adapter.

The control plane and shim use versioned local JSON IPC by default. Protocol
version `3` is carried in every request and response, and `request_id` is
echoed so callers can correlate a response. The optional management API uses
the same operations under `/api/v1`; it is disabled unless configured and
binds to loopback by default. Non-loopback listeners require a bearer token
reference. Webhooks remain independently authenticated with HMAC and are not
granted administrative API access.

`mangod` owns desired configuration, reconciliation, scheduling, execution
metadata, and history. Each `mango-shim` owns one service process tree, its
restart/stop lifecycle, logs, and durable observed state. A caller's context
deadline bounds an IPC call; service `stop_timeout` bounds graceful shutdown
before force termination.

On daemon restart, the daemon scans persisted shim state, validates the
service key, instance/incarnation identity, configuration fingerprint, and
shim handshake before attaching. A matching live shim is reused without
starting a duplicate process. A live mismatch is shut down before replacement;
an orphaned or uncertain process is surfaced as an error, while dead state is
cleaned up. This keeps process ownership with the shim while the daemon owns
the desired state.

## Security and observability

Phase 04 provides one event model across configuration, service lifecycle,
health, execution, schedule, webhook, and administrative transitions. Events
carry timestamp, actor, target, project, run ID, operation ID, configuration
generation, and safe metadata. `mango events` reads these records and
`--follow` follows by durable event ID without polling individual resources.

Administrative requests also create audit entries with actor, action, target,
result, operation ID, and time. The daemon writes JSON structured logs to
`daemon.log`, and `mango doctor`, the health response, and `/api/v1/metrics`
expose Prometheus-compatible counters/gauges for IPC requests, executions,
service state, CPU, and memory.

The management API exposes read-only health, service, execution, event, audit,
and metrics routes plus authenticated service lifecycle and execution
cancel/retry operations:

```text
GET  /api/v1/health
GET  /api/v1/services?project=demo
GET  /api/v1/services/demo/api
GET  /api/v1/executions?status=running
GET  /api/v1/events?after_id=42
GET  /api/v1/audit
GET  /api/v1/metrics
POST /api/v1/services/demo/api/restart
POST /api/v1/executions/RUN_REF/cancel
```

The API is intended for local administration or a trusted reverse proxy; Mango
does not provide public-network exposure, TLS certificate management, RBAC,
container isolation, or a security sandbox.

## Release, migration, and rollback

The formal release output is a per-platform CI artifact, not a GitHub Release.
Tagging `v*` or manually dispatching `.github/workflows/release-artifacts.yml`
builds native Windows, Linux, and macOS packages. Each archive contains the
three binaries together with `manifest.json`, the example configuration, and
the README, plus a companion `.sha256` file.

All binaries support `--version`. Go build metadata is injected with ldflags;
the Rust shim receives the same values through its build environment. The
daemon health response keeps numeric IPC `version: 3` and adds a `build` object
for version, commit, and build date. Product SemVer major versions are not
startup gates yet; IPC, shim, YAML, and metadata schema compatibility are.

Before upgrading, read [the migration guide](docs/release/migration.md). If a
database migration fails, keep the daemon stopped and use `mango doctor --json`
to inspect the marker and verified backup. Configuration rollback is
documented separately in [the rollback guide](docs/release/rollback.md) and
does not downgrade the database. The full operator checklist is in
[docs/release/checklist.md](docs/release/checklist.md), with the contract in
[docs/release/compatibility.md](docs/release/compatibility.md).

## Development

Run the test suite and static checks:

```sh
go test ./...
go test -race ./...
go vet ./...
cargo fmt --manifest-path mango-shim/Cargo.toml -- --check
cargo test --manifest-path mango-shim/Cargo.toml
cargo clippy --manifest-path mango-shim/Cargo.toml --all-targets -- -D warnings
```

Build binaries for another platform by setting `GOOS` and `GOARCH`:

```sh
GOOS=linux GOARCH=amd64 go build -o bin/mango-linux-amd64 ./cmd/mango
GOOS=linux GOARCH=amd64 go build -o bin/mangod-linux-amd64 ./cmd/mangod
GOOS=darwin GOARCH=arm64 go build -o bin/mango-darwin-arm64 ./cmd/mango
GOOS=darwin GOARCH=arm64 go build -o bin/mangod-darwin-arm64 ./cmd/mangod
GOOS=windows GOARCH=amd64 go build -o bin/mango-windows-amd64.exe ./cmd/mango
GOOS=windows GOARCH=amd64 go build -o bin/mangod-windows-amd64.exe ./cmd/mangod
cargo build --release --manifest-path mango-shim/Cargo.toml
```

The two executable entry points are:

- `cmd/mango`: CLI client.
- `cmd/mangod`: daemon entry point; its only command is `mangod run`.
