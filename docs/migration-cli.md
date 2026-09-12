# CLI migration guide

Stage 1 keeps legacy commands working while making one canonical name visible
in help and shell completion. Legacy commands write a warning to stderr, so
JSON on stdout and existing pipelines remain clean.

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
| `mango up --file PATH` | `mango up PATH` | `--file` remains accepted for scripts. |
| `mango down --project PROJECT` | `mango down PROJECT` | `--project` remains accepted for scripts. |

The `execution` and `history` namespaces are still available to existing
operators but are hidden from the default root help. Their data and IPC
behavior are unchanged; the unified `mango runs` interface is a Stage 2 task.

Target grammar is stable across the canonical commands:

- services: `PROJECT/SERVICE` (numeric service IDs remain compatibility input)
- projects: `PROJECT`
- tasks: `PROJECT/TASK`
- workflows: `PROJECT/WORKFLOW`
- logs: `PROJECT/SERVICE`, `PROJECT/task/TASK`, or
  `PROJECT/workflow/WORKFLOW/NODE`
- run controls: a full `RUN_REF` or an unambiguous prefix
