# Mango

Mango is a cross-platform service and workflow manager written in Go. It runs
long-lived processes, executes one-off tasks, connects tasks into workflows,
and triggers them on a cron schedule.

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
- A terminal monitor and interactive execution-history browsers.
- Per-user startup integration on Windows, Linux, and macOS.
- JSON output for automation and CI where supported.

## How it works

Mango has a small client/daemon architecture:

```text
                    local IPC
  mango CLI  -------------------------->  mangod
     |                                      |
     |                                      +-- service supervisor
     |                                      +-- task/workflow executor
     |                                      +-- cron scheduler
     |                                      +-- logs, health, and metrics
     |                                      +-- execution history
     |
     +-- project registry and YAML configuration
```

- `mango` is the command-line client.
- `mangod` is the background daemon. It owns process state and executes all
  service, task, workflow, and schedule operations.
- The client and daemon communicate through a Unix domain socket on Unix-like
  systems and a Windows named pipe on Windows.
- A project is a name registered to a YAML configuration file. The project
  name is stored in Mango's registry; it is not written inside the YAML file.

The main implementation areas are organized under `internal/`:

| Package | Responsibility |
| --- | --- |
| `config` | YAML v3 loading, validation, and default resolution |
| `daemon` | Project reconciliation, IPC handling, and service supervision |
| `process` | Cross-platform process and process-tree control |
| `scheduler` / `workflow` | Cron execution, task runs, DAGs, retries, and history |
| `health` / `metrics` / `logging` | Probes, process metrics, and rotating logs |
| `registry` / `startup` / `tui` | Project registry, OS startup integration, and terminal UI |

## Requirements

- Go 1.23 or newer.
- Windows, Linux, or macOS.
- The executables referenced by your configuration must be available on the
  host. Shell and Python examples in the sample configuration require the
  corresponding tools.

## Quick start

Build both binaries and keep them in the same directory:

macOS/Linux:

```sh
mkdir -p bin
go build -o bin/mango ./cmd/mango
go build -o bin/mangod ./cmd/mangod
```

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force bin
go build -o bin/mango.exe ./cmd/mango
go build -o bin/mangod.exe ./cmd/mangod
```

Start the daemon in one terminal:

```sh
./bin/mangod run
```

In Windows PowerShell, use `.\bin\mangod.exe run` and replace
`./bin/mango` in the examples below with `.\bin\mango.exe`.

In another terminal, generate, validate, register, and apply the example project:

```sh
./bin/mango init
./bin/mango config validate mango.yaml
./bin/mango project add demo mango.yaml
./bin/mango project apply demo
./bin/mango ls
```

Inspect the sample service and its logs:

```sh
./bin/mango status demo/api
./bin/mango logs demo/api --stream all --tail 50
./bin/mango logs demo/api --follow
```

To run the daemon in the background, use `mango daemon start`. This command
expects `mangod` next to `mango` or somewhere on `PATH`.

The example configuration contains services, tasks, workflows, schedules,
health checks, retries, timeouts, and dependency examples. See
[`mango.example.yaml`](mango.example.yaml) and
[`examples/tasks/README.md`](examples/tasks/README.md) for guided examples.
On Windows, adapt or remove the sample's POSIX shell and `python3` commands if
those tools are not available.

## Configuration

Mango configuration files use YAML schema version 3. Only `.yaml` files are
supported:

```yaml
version: 3

defaults:
  restart: on-failure
  stop_timeout: 10s

services:
  api:
    command: go
    args: [run, ./examples/api, --port, "8080"]
    autostart: true
    restart: always
    healthcheck:
      test: [CMD, curl, -f, "http://127.0.0.1:8080/"]

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
    tasks:
      clean:
        uses: cleanup

schedules:
  - name: nightly
    cron: "0 2 * * *"
    timezone: Asia/Taipei
    target_type: workflow
    target: nightly
```

### Services

Services represent long-running processes. A service command is executed
directly, without shell parsing. Use the shell executable explicitly when you
need shell features such as pipes or redirection.

Common service settings include:

- `command`, `args`, `working_dir`, and `environment`.
- `autostart` when the daemon starts or a project is applied.
- `restart`: `never`, `on-failure`, or `always`.
- `stop_timeout`, restart limits, and crash-loop protection.
- `healthcheck` and `depends_on`.

Health is reported separately from the service lifecycle. A failed health check
does not automatically restart the service.

### Tasks

Tasks are one-off command executions. They support their own working directory,
environment (`env`), timeout, retry policy, and concurrency mode:

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

## Command overview

The general form is:

```text
mango <command> [subcommand] [arguments] [options]
```

### Projects and configuration

```sh
mango init [PATH] [--force]
```

Creates a complete example configuration. The default output path is
`./mango.yaml`; a parent directory is created when needed. Existing files are
not overwritten unless `--force` is provided. `init` only writes the YAML; it
does not register or apply a project.

```sh
mango init
mango init ./config/mango.yaml
mango init ./mango.yaml --force
```

Validate and register the generated configuration separately:

```sh
mango config validate PATH
mango project add NAME PATH
mango project apply NAME
mango project ls
mango project rename OLD NEW
mango project remove NAME
```

`project add` stores the absolute YAML path in the registry. It does not copy
or modify the configuration file. `project apply` reloads the file and
reconciles the daemon with the new definition.

### Services

```sh
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
mango task history [PROJECT/TASK]

mango workflow ls
mango workflow run PROJECT/WORKFLOW
mango workflow status PROJECT/WORKFLOW
mango workflow history [PROJECT/WORKFLOW]

mango schedule ls
mango schedule run PROJECT/SCHEDULE
mango schedule history
```

When the target is omitted from `task history` or `workflow history` in an
interactive terminal, Mango opens a hierarchical history browser. Use
`--json` for machine-readable history and status data.

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
| `version` | integer | yes | Must be `3`. |
| `defaults` | mapping | no | Defaults shared by services and tasks. |
| `services` | mapping | no | Long-running processes keyed by service name. |
| `tasks` | mapping | no | One-off commands keyed by task name. |
| `workflows` | mapping | no | DAGs of task nodes keyed by workflow name. |
| `schedules` | sequence | no | Cron triggers targeting a task or workflow. |

### `defaults`

```yaml
defaults:
  working_dir: .
  restart: on-failure
  stop_timeout: 10s
  log_max_size: 100MiB
  log_max_files: 10
  metrics_interval: 1s
  max_restarts: 10
  restart_window: 5m
  stable_after: 1m
  inherit_env: true
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `working_dir` | string | `.` | Base working directory for services and tasks. Relative paths are resolved from the YAML file directory. |
| `restart` | string | `on-failure` | Service restart policy: `never`, `on-failure`, or `always`. |
| `stop_timeout` | duration | `10s` | Graceful-stop timeout before Mango force-stops a service. |
| `log_max_size` | size | `100MiB` | Maximum size of each service stdout/stderr log before rotation. |
| `log_max_files` | integer | `10` | Number of rotated log files to retain. |
| `metrics_interval` | duration | `1s` | Process metrics sampling interval. |
| `max_restarts` | integer | `10` | Maximum restart failures within `restart_window`. |
| `restart_window` | duration | `5m` | Time window used for crash-loop counting. |
| `stable_after` | duration | `1m` | Stable runtime after which the restart counter is cleared. |
| `inherit_env` | boolean | `true` | Whether services and tasks inherit the daemon's environment. Explicit variables override inherited values. |

Defaults are applied where the field is relevant: restart, stop timeout, log,
metrics, and crash-loop settings apply to services, while working directory
and environment inheritance also apply to tasks. Service fields override
service defaults. Task-specific settings are defined on each task; tasks do
not use service restart settings.

### `services`

Each key under `services` is a service name:

```yaml
services:
  api:
    command: go
    args: [run, ./examples/api, --port, "8080"]
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
| `args` | sequence of strings | `[]` | Arguments passed to `command`. |
| `working_dir` | string | `defaults.working_dir` or `.` | Working directory for the process. |
| `environment` | string map | inherited environment | Variables to add or override. Set `defaults.inherit_env: false` for a clean environment. |
| `autostart` | boolean | `false` | Start when the daemon starts or the project is applied. |
| `restart` | string | `defaults.restart` or `on-failure` | `never`, `on-failure`, or `always`. |
| `stop_timeout` | duration | `defaults.stop_timeout` | Graceful-stop timeout. |
| `max_restarts` | integer | `defaults.max_restarts` or `10` | Restart limit used with crash-loop protection. |
| `restart_window` | duration | `defaults.restart_window` or `5m` | Restart failure counting window. |
| `stable_after` | duration | `defaults.stable_after` or `1m` | Time required to clear the restart counter. |
| `healthcheck` | mapping | disabled | Optional command-based health monitoring. |
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
| `test` | sequence of strings | — | One probe. The first item must be `CMD`, `CMD-SHELL`, or `NONE`. |
| `policy` | string | `all` | With multiple checks, `all` requires every check to pass; `any` requires one. |
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
- `test: [NONE]` disables the health check. `NONE` is not allowed inside
  `checks`.

Probes run on the host with the service's working directory and environment.
Health is reported independently from lifecycle state; an unhealthy result
does not by itself restart the service. A single `test` is shown as the
`default` check; entries in `checks` are named `check-1`, `check-2`, and so on.

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
    env:
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
| `env` | string map | inherited environment | Variables to add or override. |
| `timeout` | duration | no limit | Starts after the process starts. A timeout force-stops the process tree and records exit code `124`. |
| `concurrency` | string | `forbid` | `forbid` queues overlapping invocations; `allow` runs them in parallel. |
| `retry` | mapping | no retries | Retry settings for failed or timed-out attempts. |

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
    tasks:
      build:
        uses: build
      test:
        uses: test
        needs: [build]
      publish:
        uses: publish
        needs: [test]
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `<workflow-name>` | mapping key | — | Unique workflow name within the project. |
| `concurrency` | string | `forbid` | `forbid` skips a new run while the same workflow is active; `allow` permits overlap. |
| `tasks` | mapping | — | At least one workflow node. |
| `<node-name>` | mapping key | — | Unique node name within the workflow. |
| `uses` | string | — | Required name of a task in the same project. |
| `needs` | sequence of strings | `[]` | Other node names that must finish successfully before this node runs. |

Independent nodes can run in parallel. If a node fails, its descendants are
skipped while independent branches continue. Workflow and task definitions are
validated for unknown references, duplicate dependencies, self-dependencies,
and cycles.

### `schedules`

Schedules trigger an existing task or workflow; they do not define a command:

```yaml
schedules:
  - name: nightly-release
    cron: "0 2 * * *"
    timezone: Asia/Taipei
    target_type: workflow
    target: release

  - name: hourly-cleanup
    cron: "0 * * * *"
    target_type: task
    target: cleanup
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | yes | Unique schedule name within the project. |
| `cron` | string | yes | Five-field standard cron expression. |
| `timezone` | string | no | IANA time zone, such as `UTC` or `Asia/Taipei`; defaults to local time. |
| `target_type` | string | yes | `task` or `workflow`. |
| `target` | string | yes | Existing task or workflow name in the same project. |

Schedules use the daemon's cron scheduler. Missed runs are not backfilled
after the daemon has been offline. In v3, schedule-level `command`, `args`,
`working_dir`, `timeout`, `retry`, and `concurrency` fields are not supported;
put execution settings on the target task instead.

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

Both forms are accepted:

```sh
mango --color=never ls
mango status demo/api --json
mango --json workflow history --tail 20
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

### `mangod`

```text
mangod run
```

`mangod` has one entry point. `run` starts the daemon in the foreground; press
Ctrl+C to stop it. The daemon owns service processes, schedules, task/workflow
execution, local IPC, logs, and execution history.

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
| `start` | none | Starts `mangod` in the background and waits for its health endpoint. `mangod` must be next to `mango` or on `PATH`. |
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
mango project apply NAME [--json]
mango project ls [--json]
```

| Command | Parameters | Description |
| --- | --- | --- |
| `add` | `NAME`, `PATH` | Validates and registers a `.yaml` file. The stored path is absolute. Duplicate names are rejected. |
| `remove` | `NAME` | Removes the project from the registry. It does not delete the YAML file, application files, or logs. A running daemon unloads the project's services and execution definitions. |
| `rename` | `OLD`, `NEW` | Re-registers the existing YAML path under a new name. It does not edit the YAML file. |
| `apply` | `NAME` | Reloads and validates the YAML, then reconciles services, tasks, workflows, and schedules. Changed or removed services are stopped; autostart services are started. |
| `ls` | none | Lists registered projects, enabled status, YAML path, config version, and last applied time. Requires the daemon. |

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
```

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

Service log files use the configured `defaults.log_max_size` and
`defaults.log_max_files` values. `logs` does not support `--json`.

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
mango task history [PROJECT/TASK] [--tail N] [--attempts] [--json]
```

| Command / flag | Description |
| --- | --- |
| `task ls` | Lists task name, command, timeout, concurrency, retry count, and latest status. |
| `task run` | Starts a task asynchronously with trigger `manual`; the command returns after the run is accepted. |
| `task history` | Lists recent task executions. Without a target, it includes tasks across projects. |
| `--tail N` | Maximum records to return; default `100`; `0` returns all retained records. Must be non-negative. |
| `--attempts` | Shows one row per retry attempt, including exit code, error, and captured stderr. |

History flags may appear before or after the optional target:

```sh
mango task history
mango task history demo/cleanup --tail 20
mango task history --attempts demo/cleanup
mango task history demo/cleanup --attempts --tail 50 --json
```

Task history records include the source (`direct` or a workflow node), trigger
(`manual` or a schedule name), start/finish times, exit code, status, error,
stderr, and attempt details. In an interactive terminal, omitting the target
opens a browser with task → runs → attempts → output levels. Use arrows or
`j`/`k` to move, `Enter` to descend, `Esc` to go back, `r` to refresh, `n` or
PageDown to load older records, and `q` to quit.

### Workflows

```text
mango workflow ls [--json]
mango workflow run PROJECT/WORKFLOW [--json]
mango workflow status PROJECT/WORKFLOW [--json]
mango workflow history [PROJECT/WORKFLOW] [--tail N] [--tasks] [--json]
```

| Command / flag | Description |
| --- | --- |
| `workflow ls` | Lists workflow name, concurrency mode, node count, and latest status. |
| `workflow run` | Starts a workflow asynchronously with trigger `manual`. |
| `workflow status` | Shows one workflow's status, node count, last run, duration, and node definitions. |
| `workflow history` | Lists recent workflow runs, optionally filtered by project/workflow. |
| `--tail N` | Maximum records; default `100`; `0` returns all retained records. Must be non-negative. |
| `--tasks` | Includes per-node task records, attempts, exit codes, and errors in text output. |

Examples:

```sh
mango workflow ls
mango workflow run demo/release
mango workflow status demo/release
mango workflow history demo/release --tail 20
mango workflow history --tasks demo/release
mango workflow history --json
```

When no workflow target is given in an interactive terminal, the history
browser navigates workflow → runs → task nodes → attempts → captured output.
Use arrows or `j`/`k` to move, `Enter` to descend, `Esc` to go back, `r` to
refresh, `n` or PageDown to load older records, and `q` to quit.

### Schedules

```text
mango schedule ls [--json]
mango schedule run PROJECT/SCHEDULE [--json]
mango schedule history [--tail N] [--attempts] [--json]
```

| Command / flag | Description |
| --- | --- |
| `schedule ls` | Lists schedule, cron expression, time zone, target type, target, and next run. |
| `schedule run` | Triggers a configured schedule immediately; the target runs asynchronously. |
| `schedule history` | Lists recent execution records retained by the daemon. |
| `--tail N` | Maximum history records; default `100`; `0` returns all retained records. Must be non-negative. |
| `--attempts` | Shows one row per recorded attempt with timing, result, error, and stderr. |

Examples:

```sh
mango schedule ls
mango schedule run demo/nightly-release
mango schedule history --tail 20
mango schedule history --attempts --json
```

`schedule history` is an aggregate compatibility view. Use `task history` or
`workflow history` when you need to filter by a specific target type or inspect
workflow nodes.

### Startup integration

```text
mango startup install
mango startup uninstall
mango startup status [--json]
```

| Command | Description |
| --- | --- |
| `install` | Installs per-user daemon startup integration for the current OS. |
| `uninstall` | Removes the integration. It does not remove project files or logs. |
| `status` | Reports whether integration is installed and where it is configured. |

The platform mechanisms are Windows Task Scheduler, Linux `systemd --user`,
and a macOS `launchd` LaunchAgent. Startup integration launches only `mangod`;
service startup still depends on each service's `autostart` setting. The daemon
binary must be available next to `mango` or on `PATH` when installing.

```sh
mango startup install
mango startup status
mango startup uninstall
```

### `mango doctor`

```text
mango doctor [--json]
```

Checks and prints the current platform, Mango root, registry path, log root,
daemon log, execution-history path, registry readability, daemon status,
resolved `mangod` path, and startup integration status. It is useful when a
daemon cannot start or a project is missing from the service list.

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
go run ./examples/api [--port PORT] [--interval DURATION]
```

| Option | Default | Description |
| --- | --- | --- |
| `--port` | `8080` | HTTP listening port from `1` to `65535`. |
| `--interval` | `5s` | Heartbeat interval. |

The server provides `GET /` for a successful response and `GET /error` for an
intentional HTTP 500 plus stderr output.

```sh
go run ./examples/api --port 8080 --interval 3s
curl http://127.0.0.1:8080/
curl http://127.0.0.1:8080/error
```

### One-off task

```text
go run ./examples/one-task [--iterations N] [--interval DURATION] [--fail]
```

| Option | Default | Description |
| --- | --- | --- |
| `--iterations` | `5` | Number of iterations; must be at least `1`. |
| `--interval` | `500ms` | Delay between iterations. |
| `--fail` | off | Exit with code `2` after writing output. |

Example:

```sh
go run ./examples/one-task --iterations 3 --interval 200ms --fail
```

### Workflow task helpers

`examples/tasks/emit.go` and `examples/tasks/transform.py` accept the same
logical options:

```text
emit.go / transform.py [--name NAME] [--steps N] [--delay DURATION_OR_SECONDS] [--fail]
```

| Option | Go default | Python default | Description |
| --- | --- | --- | --- |
| `--name` | `go-task` | `python-task` | Name written to the task log. |
| `--steps` | `3` | `3` | Number of steps; must be positive. |
| `--delay` | `250ms` | `0.25` | Delay between steps. Python uses seconds as a float. |
| `--fail` | off | off | Write a failure message and exit with code `7`. |

The shell helpers accept positional arguments:

```text
load.sh [NAME]
fail.sh [NAME]
slow.sh [NAME] [SECONDS]
```

`fail.sh` exits with code `7`; `slow.sh` is useful for testing task timeouts.
`run_api.sh` has no options and starts example API servers on ports `9000` and
`9090`. These scripts require a POSIX shell.

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
│   ├── daemon.pid
│   └── mango.sock              # Unix; Windows uses a named pipe
├── logs/
└── state/
    └── execution-history.json
```

The optional `daemon.yaml` currently supports `schedule_history_limit`.

```yaml
# MANGO_HOME/daemon.yaml
schedule_history_limit: 1000
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `schedule_history_limit` | non-negative integer | `0` | Maximum number of execution records to retain. `0` keeps all records. |

## Platform behavior

- Unix systems stop services through their process group and force-stop them
  after the configured timeout.
- Windows uses Job Objects to manage a service's process tree.
- `mango startup install` uses Windows Task Scheduler, Linux `systemd --user`,
  or a macOS `launchd` LaunchAgent.

## Development

Run the test suite and static checks:

```sh
go test ./...
go test -race ./...
go vet ./...
```

Build binaries for another platform by setting `GOOS` and `GOARCH`:

```sh
GOOS=linux GOARCH=amd64 go build -o bin/mango-linux-amd64 ./cmd/mango
GOOS=linux GOARCH=amd64 go build -o bin/mangod-linux-amd64 ./cmd/mangod
GOOS=darwin GOARCH=arm64 go build -o bin/mango-darwin-arm64 ./cmd/mango
GOOS=darwin GOARCH=arm64 go build -o bin/mangod-darwin-arm64 ./cmd/mangod
GOOS=windows GOARCH=amd64 go build -o bin/mango-windows-amd64.exe ./cmd/mango
GOOS=windows GOARCH=amd64 go build -o bin/mangod-windows-amd64.exe ./cmd/mangod
```

The two executable entry points are:

- `cmd/mango`: CLI client.
- `cmd/mangod`: daemon entry point; its only command is `mangod run`.
