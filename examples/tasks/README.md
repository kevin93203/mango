# Mango workflow examples

These tasks are intentionally small and deterministic so they can be used to
exercise the v3 Tasks / Workflows / Schedules configuration in
[`mango.example.yaml`](../../mango.example.yaml).

From the repository root, validate the configuration and inspect the
definitions:

```sh
mango config validate mango.example.yaml
mango task ls
mango workflow ls
```

After starting the Mango daemon with this configuration, run examples
manually so you do not need to wait for the cron trigger:

The commands below assume the project is registered with the name `demo`;
replace that prefix with your project name if needed.

```sh
mango task run demo/demo-direct
mango task run demo/demo-artifact
mango workflow run demo/demo-linear
mango workflow run demo/demo-parallel
mango workflow run demo/demo-failure
mango workflow run demo/demo-timeout
```

Inspect execution history, including per-node attempts:

```sh
mango history --target-type workflow --target demo/demo-failure --attempts
mango history --target-type task --target demo/demo-slow-timeout --attempts
```

The failure workflow records retries for `demo-retry-failure`; its dependent
node is skipped while the independent branch continues. The timeout workflow
forces `demo-slow-timeout` to stop after two seconds and records exit code 124.

`demo-artifact` runs `examples/tasks/artifact/main.go`, writes
`examples/artifacts/demo-artifact.txt`, and declares that file in `outputs`.
Use `mango history show RUN_ID --json` to inspect its existence, size, and
SHA-256 metadata.

All task helpers used by the sample are Go programs and run on Windows 10/11,
macOS, and Linux. The HTTP service healthchecks require `curl` to be available
on each platform.
