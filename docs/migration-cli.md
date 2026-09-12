# CLI migration guide

Stages 1 and 2 keep legacy commands working while making one canonical name
visible in help and shell completion. Legacy commands write a warning to
stderr, so JSON on stdout and existing pipelines remain clean.

| Legacy command | Canonical command | Notes |
| --- | --- | --- |
| `mango ps` | `mango service list` | Same service-list handler; `ps` is hidden. |
| `mango ls` | `mango service list` | Same service-list handler; `ls` is hidden. |
| `mango start TARGET...` | `mango service start TARGET...` | Root shortcut remains supported. |
| `mango stop TARGET...` | `mango service stop TARGET...` | Root shortcut remains supported. |
| `mango restart TARGET...` | `mango service restart TARGET...` | Root shortcut remains supported. |
| `mango enable TARGET...` | `mango service enable TARGET...` | Root form is compatibility-only. |
| `mango disable TARGET...` | `mango service disable TARGET...` | Root form is compatibility-only. |
| `mango project add NAME PATH` | `mango project register NAME PATH` | Registry-only operation; no YAML is changed. |
| `mango project ls` | `mango project list` | `ls` is hidden. |
| `mango task ls` | `mango task list` | `ls` is hidden. |
| `mango workflow ls` | `mango workflow list` | `ls` is hidden. |
| `mango schedule ls` | `mango schedule list` | `ls` is hidden. |
| `mango task run PROJECT/TASK` | `mango run task PROJECT/TASK` | Add `--wait` when the terminal result is needed. |
| `mango workflow run PROJECT/WORKFLOW` | `mango run workflow PROJECT/WORKFLOW` | Add `--wait` when the terminal result is needed. |
| `mango execution list` | `mango runs list` | `runs list` includes active and terminal runs by default. |
| `mango execution ls` | `mango runs list` | Same filters; `--all` becomes the default scope. |
| `mango execution get REF` | `mango runs show REF` | Shows active metadata or terminal detail for the same run reference. |
| `mango execution watch REF` | `mango runs watch REF` | Same timeout and run-reference rules. |
| `mango execution cancel REF` | `mango runs cancel REF` | Same cancellation behavior. |
| `mango execution retry REF` | `mango runs retry REF` | Same new run ID and `retried_from_run_id` lineage. |
| `mango execution logs REF` | `mango runs logs REF` | Same stream and tail flags. |
| `mango history list` | `mango runs list` | Use `--status` or filters; terminal-only history is no longer the default scope. |
| `mango history ls` | `mango runs list` | Same migration as `history list`. |
| `mango history show REF` | `mango runs show REF` | The canonical command also accepts active runs. |
| `mango history purge ...` | `mango runs prune ...` | `--yes` remains required; only terminal runs are removed. |
| `mango up --file PATH` | `mango up PATH` | `--file` remains accepted for scripts. |
| `mango down --project PROJECT` | `mango down PROJECT` | `--project` remains accepted for scripts. |

Stage 2 does not remove any legacy command. The execution/history aliases stay
through the compatibility cycle and are candidates for removal at the Stage 3
major-release boundary.

The `execution` and `history` namespaces are still available to existing
operators but are hidden from the default root help. Their data and IPC
behavior are unchanged; each legacy leaf command writes its replacement to
stderr. JSON remains on stdout without the warning.

Target grammar is stable across the canonical commands:

- services: `PROJECT/SERVICE` (numeric service IDs remain compatibility input)
- projects: `PROJECT`
- tasks: `PROJECT/TASK`
- workflows: `PROJECT/WORKFLOW`
- logs: `PROJECT/SERVICE`, `PROJECT/task/TASK`, or
  `PROJECT/workflow/WORKFLOW/NODE`
- run controls: a full `RUN_REF` or an unambiguous prefix

`mango runs` lists active and terminal runs together, newest first. `--active`
limits the list to queued and running runs; `--limit 0` means all matching
runs. `mango runs prune` requires `--yes` and never removes active runs, logs,
or lifetime counters.

In an interactive terminal, `mango runs` and `mango runs list` use the same
hierarchical browser as `mango history`, including manual `r` refresh and
lazy terminal detail loading. Active rows use only `execution.ls` metadata;
opening a terminal row fetches its existing `history.get` detail once per TUI
session. JSON and non-TTY output remain list-oriented, and the compatibility
`mango history` browser remains terminal-only.
