# Mango CLI migration guide

Stage 3 is the major-release cutover to the canonical CLI. The old command
names below are no longer executed by the new `mango` binary. They return a
non-zero migration error on stderr, do not contact the daemon, and are absent
from help and shell completion. JSON pipelines therefore keep stdout empty on
failure.

The migration error path is intentionally kept for this major release. If a
rollback requires an old command, install the previous compatible package (or
its compatibility CLI), stop the current daemon with the matching CLI, and
follow [the rollback guide](release/rollback.md). Replacing only the CLI does
not change the registry, generations, logs, history, or run references.

## Removed command map

| Old syntax | New syntax | Behavior and boundary |
| --- | --- | --- |
| `mango ps` | `mango service list` | Removed in the Stage 3 major release; the error points to the same service-list operation. |
| `mango ls` | `mango service list` | Removed in the Stage 3 major release; no service operation is run. |
| `mango start TARGET...` | `mango service start TARGET...` | Root `start` remains a supported shortcut; use the noun-first form in new automation. |
| `mango stop TARGET...` | `mango service stop TARGET...` | Root `stop` remains a supported shortcut; use the noun-first form in new automation. |
| `mango restart TARGET...` | `mango service restart TARGET...` | Root `restart` remains a supported shortcut; use the noun-first form in new automation. |
| `mango enable TARGET...` | `mango service enable TARGET...` | Removed in the Stage 3 major release. |
| `mango disable TARGET...` | `mango service disable TARGET...` | Removed in the Stage 3 major release. |
| `mango project add NAME PATH` | `mango project register NAME PATH` | Removed in the Stage 3 major release; registry-only registration is still available under `register`. |
| `mango project ls` | `mango project list` | Removed in the Stage 3 major release. |
| `mango service ls [PROJECT]` | `mango service list [PROJECT]` | Removed in the Stage 3 major release. |
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
mango status PROJECT/SERVICE
mango logs TARGET [--follow]
mango run task PROJECT/TASK [--wait]
mango run workflow PROJECT/WORKFLOW [--wait]
mango runs [list|show|watch|cancel|retry|logs|prune]
mango service [list|status|start|stop|restart|enable|disable]
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
