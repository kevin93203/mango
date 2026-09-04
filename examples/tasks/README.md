# Mango workflow examples

These tasks are intentionally small and deterministic so they can be used to
exercise the v3 Tasks / Workflows / Schedules configuration in
[`mango.example.yaml`](../../mango.example.yaml).

From the repository root, validate the configuration and inspect the
definitions:

```sh
go run ./cmd/mango config validate mango.example.yaml
go run ./cmd/mango task ls
go run ./cmd/mango workflow ls
```

After starting the Mango daemon with this configuration, run examples
manually so you do not need to wait for the cron trigger:

The commands below assume the project is registered with the name `demo`;
replace that prefix with your project name if needed.

```sh
go run ./cmd/mango task run demo/demo-direct-python
go run ./cmd/mango workflow run demo/demo-linear
go run ./cmd/mango workflow run demo/demo-parallel
go run ./cmd/mango workflow run demo/demo-failure
go run ./cmd/mango workflow run demo/demo-timeout
```

Inspect execution history, including per-node attempts:

```sh
go run ./cmd/mango workflow history --tasks demo/demo-failure
go run ./cmd/mango task history demo/demo-slow-timeout
```

The failure workflow records retries for `demo-retry-failure`; its dependent
node is skipped while the independent branch continues. The timeout workflow
forces `demo-slow-timeout` to stop after two seconds and records exit code 124.

The sample uses `python3` and `sh` commands. On systems where Python is named
`python`, change that command in the YAML; the shell tasks require a POSIX
shell.
