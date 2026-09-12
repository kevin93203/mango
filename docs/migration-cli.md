# Mango CLI migration guide

Stage 3 is the major-release cutover to the canonical CLI. Most old command
names below return a non-zero migration error on stderr, do not contact the
daemon, and are absent from help and shell completion. The service namespace
and the `ps`/`ls` listing aliases are intentionally unregistered instead, so
they return Cobra's unknown-command error. JSON pipelines keep stdout empty on
failure in both cases.

The migration error path is intentionally kept for this major release. If a
rollback requires an old command, install the previous compatible package (or
its compatibility CLI), stop the current daemon with the matching CLI, and
follow [the rollback guide](release/rollback.md). Replacing only the CLI does
not change the registry, generations, logs, history, or run references.

## Removed command map

| Old syntax | New syntax | Behavior and boundary |
| --- | --- | --- |
| `mango ps` | `mango list` | Fully removed; no compatibility alias is registered. |
| `mango ls` | `mango list` | Fully removed; no compatibility alias is registered. |
| `mango service list [PROJECT]` | `mango list [PROJECT]` | The `service` namespace is fully removed; use the root service-list command. |
| `mango service ls [PROJECT]` | `mango list [PROJECT]` | The `service` namespace and nested `ls` alias are fully removed. |
| `mango service status TARGET` | `mango status TARGET` | The root command keeps the same target and watch behavior. |
| `mango service start TARGET...` | `mango start TARGET...` | Root `start` is now the canonical service command. |
| `mango service stop TARGET...` | `mango stop TARGET...` | Root `stop` is now the canonical service command. |
| `mango service restart TARGET...` | `mango restart TARGET...` | Root `restart` is now the canonical service command. |
| `mango service enable TARGET...` | `mango enable TARGET...` | Root `enable` is now the canonical persistent service-policy command. |
| `mango service disable TARGET...` | `mango disable TARGET...` | Root `disable` is now the canonical persistent service-policy command. |
| `mango project add NAME PATH` | `mango project register NAME PATH` | Removed in the Stage 3 major release; registry-only registration is still available under `register`. |
| `mango project ls` | `mango project list` | Removed in the Stage 3 major release. |
| `mango task ls` | `mango task list` | Removed in the Stage 3 major release. |
| `mango task run PROJECT/TASK` | `mango run task PROJECT/TASK` | Removed in the Stage 3 major release; add `--wait` when the terminal result is needed. |
| `mango workflow ls` | `mango workflow list` | Removed in the Stage 3 major release. |
| `mango workflow run PROJECT/WORKFLOW` | `mango run workflow PROJECT/WORKFLOW` | Removed in the Stage 3 major release; add `--wait` when the terminal result is needed. |
| `mango schedule ls` | `mango schedule list` | Removed in the Stage 3 major release. |
| `mango schedule history` | `mango runs list --trigger-type schedule` | Removed with the schedule-specific history endpoint. |
| `mango execution` | `mango runs` | The whole namespace is removed; use the matching `runs` action below. |
| `mango execution list` / `ls` | `mango runs list` | Active and terminal runs use one list scope. |
| `mango execution get REF` | `mango runs show REF` | Shows active metadata or terminal detail. |
| `mango execution watch REF` | `mango runs watch REF` | The run-reference and timeout rules are unchanged. |
| `mango execution cancel REF` | `mango runs cancel REF` | Cancellation behavior is unchanged. |
| `mango execution retry REF` | `mango runs retry REF` | A new run ID is created and linked with `retried_from_run_id`. |
| `mango execution logs REF` | `mango runs logs REF` | Stream and tail validation are unchanged. |
| `mango history` | `mango runs` | The terminal-only compatibility browser is removed. |
| `mango history list` / `ls` | `mango runs list` | Use `--status` or filters instead of a terminal-only scope. |
| `mango history show REF` | `mango runs show REF` | The canonical command also accepts active runs. |
| `mango history purge ...` / `clear` | `mango runs prune ...` | `--yes` remains required; only terminal metadata is pruned. |

`mango up --file PATH` and `mango down --project PROJECT` remain accepted
compatibility flag forms. New scripts should use `mango up PATH` and
`mango down PROJECT`.

## Canonical surface

Use these commands in README examples, CI, packages, and new scripts:

```text
mango init [PATH]
mango up [PATH]
mango down [PROJECT]
mango list [PROJECT]
mango status PROJECT/SERVICE
mango start TARGET [TARGET ...]
mango stop TARGET [TARGET ...]
mango restart TARGET [TARGET ...]
mango enable TARGET [TARGET ...]
mango disable TARGET [TARGET ...]
mango logs TARGET [--follow]
mango run task PROJECT/TASK [--wait]
mango run workflow PROJECT/WORKFLOW [--wait]
mango runs [list|show|watch|cancel|retry|logs|prune]
mango project [list|plan|apply|status|rollback|register|remove|rename]
mango schedule [list|enable|disable]
mango config validate PATH
mango daemon [start|stop|restart|status|logs]
mango doctor
```

Targets use stable names such as `PROJECT/SERVICE`, `PROJECT/TASK`, and
`PROJECT/WORKFLOW`. Run controls accept a full run reference or an unambiguous
prefix. JSON always contains complete run IDs and migration errors never put
warnings or progress text on JSON stdout.
