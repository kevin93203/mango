# Phase 07: CLI Simplification and Progressive Migration

Status: Stage 2 complete; Stage 3 in progress.

## Objective

降低 Mango CLI 的使用者心智模型，讓使用者以少數幾個任務導向的入口完成大部分
日常工作，同時保留進階維運能力與既有腳本的相容性。

本計畫的核心原則是：

- 不因簡化 CLI 而刪除 Mango 的 runtime、workflow、schedule 或 rollback 能力。
- 將內部架構名詞從新手入口移除，尤其是 daemon、execution/history split、
  registry 與 generation。
- 每個概念只保留一個 canonical command name；舊名稱先作為相容入口，再逐步退場。
- 高頻操作使用短且可預測的命令，低頻操作集中到資源型或進階 namespace。
- CLI 的輸出、target grammar、JSON 行為與錯誤訊息都必須形成穩定的公開契約。

## Baseline

本計畫以目前 working tree 的實作為基準，不以早期 plan 或舊文件中的命令列表
作為真實來源。Cobra root command 的註冊位於
[cmd/mango/cobra.go](../cmd/mango/cobra.go)，README 也將 Cobra tree 定義為
命令語法的權威來源。

目前的使用者入口如下：

- mango 有 24 個明確的 top-level commands，另有內建 help 與 --version。
- mango 約有 49 個可執行的 leaf actions。
- mangod 對外只有 mangod run --home PATH；它是 daemon 執行入口，
  不應成為一般使用者的主要心智模型。
- mango ps 與 mango ls 都是列出 service，且目前走相同的 compose-style
  command path。
- task/workflow 啟動、active execution、terminal history 分別由三組命令表達：
  task/workflow run、execution、history。
- mango up 目前要求 daemon 已經啟動；README quick start 因此需要先執行
  mango daemon start。

目前的 CLI 仍需保留下列技術相容性：

- 現有 YAML schema、project registry、generation snapshots、logs 與 history
  資料不可因 CLI 重整而被刪除或自動改寫。
- 現有 IPC endpoints 可繼續存在；CLI simplification 優先在 command adapter
  層完成，只有統一 run 查詢需要時才新增 daemon-side facade。
- run_id 的完整值、unique prefix resolution、retry lineage 與 workflow child
  linkage 必須保持可追蹤。

## Three-stage roadmap

    Stage 1
    Canonical UX + compatibility foundation
            ↓
    Stage 2
    Unified run/runs model
            ↓
    Stage 3
    Major-release cleanup and removal

前一版分析中的「資訊架構準備」被併入 Stage 1，不另外算作獨立產品階段。

---

## Stage 1: Canonical UX and Compatibility Foundation

### Goal

先建立一致的命令資訊架構、target grammar、help 與相容層，不改變既有資料格式，
讓新使用者從根命令 help 就能找到日常入口。

### User-facing target

Stage 1 的新手入口建議固定為：

    mango init [PATH]
    mango up [PATH]
    mango status [TARGET]
    mango logs TARGET [--follow]
    mango run task TARGET [--wait]
    mango run workflow TARGET [--wait]
    mango down [PROJECT]

`mango runs ...` is intentionally deferred to Stage 2; the existing hidden
`execution` and `history` namespaces remain compatibility interfaces until
the unified run facade is implemented.

資源與進階命令則使用一致的 noun-first 形式：

    mango service list|status|start|stop|restart|enable|disable
    mango project list|plan|apply|status|rollback|register|remove|rename
    mango task list
    mango workflow list
    mango schedule list|enable|disable
    mango config validate
    mango daemon start|stop|restart|status|logs
    mango doctor

events、monitor、startup 可在 Stage 1 先保留原入口，但必須在 help 中歸類到
Advanced 區段。是否在 Stage 3 移到 system 或其他進階 namespace，留待實際使用
資料與相容性評估後決定。

### Command naming decisions

| Concept | Canonical name | Compatibility behavior |
| --- | --- | --- |
| Service listing | mango service list | mango ps、mango ls 保留為 hidden/deprecated aliases |
| Service operations | mango service start, stop, restart | root start/stop/restart 保留為高頻 shortcuts |
| Persistent service policy | mango service enable/disable | root enable/disable 移到 compatibility layer |
| Resource listing | list | 各 namespace 的 ls 轉為 alias，不再新增新的 ls |
| Task execution | mango run task TARGET | mango task run TARGET 保留 |
| Workflow execution | mango run workflow TARGET | mango workflow run TARGET 保留 |
| Configuration check | mango config validate PATH | 可增加 mango check PATH shortcut |
| Project lifecycle | mango up / mango down | project add/apply 作為 advanced operator interface |

不建議同時將 ps、ls、list、services 都列為正式入口。若要提供更短的快捷命令，
應選擇一個並在 help 中清楚標示其 scope。

### Detailed implementation work

#### 1. Command taxonomy and help

- 在 root help 中加入明確的 command groups：

      Start here:
        init, up, status, logs, run, down

      Manage:
        service, project, task, workflow, schedule

      Advanced:
        config, daemon, events, monitor, startup, doctor

- 所有 command short description 改成一致的動詞格式，例如：
  List services、Start one or more services、Show service status。
- namespace 不帶 subcommand 時，顯示「最常用操作 + 範例」，不要只顯示原始
  的 Cobra generic help。
- root help 增加 3–5 個可直接複製的 quick-start examples。
- 重新啟用 shell completion，至少支援 Bash、Zsh、Fish 與 PowerShell。
- 為 command tree 加入 snapshot-style tests，確保不會意外重新暴露 deprecated
  commands。

#### 2. Add canonical aliases without changing backend behavior

- 將 task ls、workflow ls、schedule ls、project ls 對應到完整的 list command。
- 將 task run 與 workflow run 對應到新的 run task 與 run workflow。
- 將 root service commands 包裝到 service namespace；root shortcuts 仍呼叫
  相同 handler。
- 將 ps 與 ls 統一導向同一個 service-list handler，避免兩條 command path
  再次產生語意差異。
- deprecated commands 必須：

  - 保持相同 positional arguments、exit status 與 stdout 格式。
  - 將 migration warning 寫到 stderr，不污染 JSON 或 pipe output。
  - 在 --help 中不再作為主要 command 顯示。
  - 錯誤訊息指向 canonical replacement。

#### 3. Simplify project lifecycle

- mango up [PATH] 新增 positional config path；--file PATH 保留為相容形式。
- mango down [PROJECT] 支援從目前目錄的 config 或明確 project name 解決目標；
  --project 保留為相容形式。
- up 的公開語意定義為「讀取、驗證、註冊或更新、apply，並等待 project ready」。
- down 的公開語意定義為「停止並停用 project，但不刪除 registry、config、logs、
  generation snapshots 或 history」。
- project add/apply/plan/status/rollback 不刪除，但在文件中明確標示為 advanced
  desired-state operations。

#### 4. Hide daemon architecture from the happy path

- mango up 預設在 daemon 不可用時嘗試啟動 mangod，並等待 health endpoint。
- 增加 --no-daemon，供 systemd、CI、受管控環境與需要明確 daemon ownership 的
  使用者停用自動啟動。
- mango daemon start 仍保留，且維持 idempotent；它是維運命令，不是 quick start
  必要步驟。
- mango up 找不到 mangod 時，錯誤必須指出安裝／PATH 問題與可用的修復命令。
- status、logs、doctor 不應因為讀取資訊而隱式修改 project 或啟動服務。

#### 5. Establish target grammar

Stage 1 應新增共用 target parser 或 resolver，避免各 command 自行解讀字串：

    service target: PROJECT/SERVICE
    project target: PROJECT
    task target: PROJECT/TASK
    workflow target: PROJECT/WORKFLOW
    log target: PROJECT/SERVICE, PROJECT/task/TASK,
                PROJECT/workflow/WORKFLOW/NODE
    run target: RUN_REF or unique RUN_REF prefix

規則：

- stable names 是文件與輸出的主要形式；numeric service ID 只作相容便利功能，
  不再作為主要範例。
- target 不明確時，命令不得猜測；錯誤訊息應列出可用的 canonical forms。
- 若未來允許 mango run TARGET 省略 task/workflow kind，只有在唯一匹配時才可
  推斷；多重匹配必須要求使用者補上 task 或 workflow。
- project-local commands 可從目前目錄找到 mango.yaml，但不得建立隱式且持久的
  global context。

#### 6. Normalize flags and output

- 定義 global flags 的真正適用範圍；不支援的 command 不應在 help 中顯示該 flag。
- 非互動 read/mutate commands 優先全面支援 --json。
- monitor 與 follow-style interactive UI 可維持非 JSON，但必須在 help 中清楚
  說明替代的 machine-readable command。
- --no-trunc 應只在輸出 run references 的 commands 中顯示；JSON 永遠輸出完整 ID。
- --tail、--limit 的 default 與 0 語意需在所有相關 command 統一。
- --wait 成為 asynchronous operation 的一致選項；up 可維持預設等待 ready，
  run 則預設回傳 accepted run reference，使用 --wait 等待 terminal result。
- daemon unavailable 錯誤不應硬編碼成只有 mango daemon start 一條路；依 command
  context 顯示 mango up 或 advanced daemon recovery suggestion。

### Stage 1 API and data impact

- 不修改 YAML schema。
- 不修改 registry、generation snapshots、logs 或 history tables。
- 不刪除既有 IPC endpoints。
- 可新增 CLI-only adapter layer，以及 daemon auto-start helper。
- 如需修改 ErrDaemonUnavailable，應改為結構化 error code，讓 CLI 根據目前 command
  顯示不同 recovery hint，而不是在 internal package 內固定一段完整 CLI 文案。

### Stage 1 tests

- Root command tree：canonical commands 存在，deprecated commands 不出現在預設 help。
- 每一個 canonical command 與 compatibility alias 都有 syntax、flag、exit-status
  測試。
- mango up PATH 與 mango up --file PATH 行為一致。
- mango up 在 daemon stopped、daemon already running、mangod missing、daemon
  health timeout 時的錯誤與 recovery hint。
- mango down PROJECT、目前目錄 project resolution 與不刪除資料的行為。
- target parser 的 service/project/task/workflow/run ambiguity tests。
- JSON output 不混入 deprecation warning、ANSI 或 progress text。
- Bash、Zsh、Fish、PowerShell completion 的 command/flag smoke tests。
- Windows、Linux、macOS 的 daemon executable discovery 與 auto-start tests。

### Stage 1 documentation

- README quick start 改為不需要手動執行 mango daemon start：

      mango init
      mango up
      mango status demo/api
      mango logs demo/api --follow
      mango down

- README 的 command reference 只以 canonical commands 作為主要範例。
- 新增 Advanced commands 章節，說明 project apply、daemon、startup、events
  與 rollback 的適用情境。
- 修正 README 中 schema version 3 與 version 4 的矛盾，並確認文件與目前
  internal/config 的 validation behavior 一致。
- 新增 CLI migration table，將每個舊命令指向 replacement。

### Stage 1 acceptance criteria

- Root help 有清楚的 Start here、Manage、Advanced 分組。
- 新手不必先理解 mangod、registry 或 generation 就能完成 init → up → status。
- ps 與 ls 不再作為兩個主要入口出現在 help。
- 所有 list commands 使用 list 作為 canonical 名稱。
- command help 不會宣稱支援實際會被拒絕的 global flag。
- 舊 command 的 script behavior 在 compatibility mode 下維持不變。

---

## Stage 2: Unified run / runs Model

### Goal

將 task/workflow execution、active execution 與 terminal history 統一成使用者
可理解的單一概念：run。

使用者不應需要知道某一筆資料目前存在於 execution store 還是 history read model；
只需要知道它的 target、run reference、status 與 output。

### Canonical command surface

    mango run task PROJECT/TASK [--wait]
    mango run workflow PROJECT/WORKFLOW [--wait]

    mango runs
    mango runs list
    mango runs show RUN_REF
    mango runs watch RUN_REF [--timeout DURATION]
    mango runs cancel RUN_REF
    mango runs retry RUN_REF
    mango runs logs RUN_REF [--stream STREAM] [--tail N]
    mango runs prune (--before RFC3339 | --all) --yes

mango runs 不帶 subcommand 時等同於 mango runs list，提供最短的日常查詢入口。

可選的第二階段便利語法：

    mango run PROJECT/TASK
    mango run PROJECT/WORKFLOW
    mango logs RUN_REF

這些 shorthand 只有在 target 唯一可判斷時才啟用；第一版仍以明確的
run task／run workflow 作為 canonical syntax。

### Unified list semantics

mango runs list 的 canonical behavior：

- 預設列出最近的 active 與 terminal runs，使用一致的 RUN_ID、target、status、
  started、elapsed 欄位。
- --status STATUS 可篩選 queued、running、succeeded、failed、cancelled 等狀態。
- --active 是需要只看 queued/running 時的明確 shortcut；不再讓 --all 決定
  execution/history 的資料邊界。
- --limit N 在所有 run list commands 使用相同 default 與 0 語意。
- --project、--target、--target-type、--trigger-type、--trigger 保留 filter 能力，
  但在 help 中分成 Basic filters 與 Advanced filters。
- workflow 的 root run 是 list 的主要 row；child task/node 詳細資料由
  mango runs show RUN_REF 或 --attempts 取得。
- history 的 terminal-only ordering 與 execution 的 active-only ordering 不再
  暴露為兩種 user-facing command semantics。

### Unified run detail semantics

mango runs show RUN_REF 應能處理 active 與 terminal run：

- active run 顯示目前 status、started time、elapsed、target 與可用的 live metadata。
- terminal run 顯示 finished time、exit/status、attempts、workflow nodes 與 lifecycle
  events。
- retry 顯示新 run reference 與 retried_from_run_id 關係。
- ambiguous prefix 必須在執行 cancel/retry 等 mutation 前失敗，並列出候選 refs。
- human-readable output 可以縮短 ID；JSON 永遠輸出 canonical full ID。

### Daemon and IPC design

建議分兩步完成，以降低風險。

#### Stage 2A: CLI facade

- 新的 runs command 先在 CLI 層統一輸入、輸出與錯誤格式。
- 可以暫時呼叫既有 execution.ls、execution.watch、history.ls、history.get
  等 endpoints。
- 既有 execution 與 history endpoints 保持不變，作為 compatibility path。
- 這一步先驗證 command UX、JSON schema 與 migration behavior。

#### Stage 2B: Daemon-side run facade

若 CLI 組合既有 responses 造成 ordering、filter 或 consistency 問題，新增 versioned
daemon methods：

    runs.list
    runs.get
    runs.watch
    runs.cancel
    runs.retry
    runs.logs
    runs.prune

規則：

- 新 facade 使用既有 execution/history tables，不另建第二套 run database。
- 舊 IPC methods 繼續服務舊 CLI 與舊版本 client，直到 major release cleanup。
- 新 response 必須有明確 schema version 或穩定 field contract。
- retry 的新 run ID 與 retried_from_run_id 行為沿用目前 major-release contract。
- prune 仍只刪 terminal metadata，不能刪除 active metadata、lifetime counters、
  logs 或 project configuration。

#### Stage 2C: Shared interactive run browser

在進入 Stage 3 前，`mango runs` 與 `mango runs list` 已完成 `mango history` 的
互動式瀏覽器移植，兩個 canonical 入口在 interactive terminal 中提供相同的
`run → task` 或 `run → workflow → tasks → attempts → output` navigation。

- 根列表透過 `execution.ls` 同時顯示 active 與 terminal runs；`r` 只做手動
  refresh，不進行背景 polling。
- Enter 開啟 terminal run 時才呼叫既有 `history.get`，並在 TUI session 內快取
  detail；active run 只顯示 `execution.ls` 提供的 metadata。
- terminal detail 保留既有 attempts、workflow node、safe command metadata、
  retained stdout/stderr 與分頁鍵盤操作。
- `--json` 與 non-TTY output 維持既有 list/schema behavior；`mango history`
  仍是 terminal-only compatibility command。
- `--target-type task --target PROJECT/TASK` 同時匹配 direct task root 與包含
  該 task 的 workflow root，detail 頁只顯示匹配的 workflow nodes。

此階段不新增 daemon IPC facade、資料表或 schema；先以既有 execution/history
endpoints 完成一致的 user-facing interaction。

### Logs integration

- mango runs logs RUN_REF 是 run reference 的 canonical log command。
- mango logs TARGET 繼續服務 service、task、workflow-node target。
- 若實作成本可控，增加 mango logs RUN_REF，由 resolver 自動辨識 run reference；
  否則在 mango run 成功輸出中明確提供 mango runs logs RUN_REF。
- service logs 與 execution logs 的 --stream、--tail default 必須分別文件化，
  但命名與 validation 規則應一致。

### Stage 2 compatibility mapping

| Existing command | Canonical replacement | Compatibility period |
| --- | --- | --- |
| mango task run TARGET | mango run task TARGET | Keep and deprecate |
| mango workflow run TARGET | mango run workflow TARGET | Keep and deprecate |
| mango execution ls | mango runs list | Keep and deprecate |
| mango execution get REF | mango runs show REF | Keep and deprecate |
| mango execution watch REF | mango runs watch REF | Keep and deprecate |
| mango execution cancel REF | mango runs cancel REF | Keep and deprecate |
| mango execution retry REF | mango runs retry REF | Keep and deprecate |
| mango execution logs REF | mango runs logs REF | Keep and deprecate |
| mango history ls | mango runs list | Keep and deprecate |
| mango history show REF | mango runs show REF | Keep and deprecate |
| mango history purge ... | mango runs prune ... | Keep and deprecate |

在 Stage 2 結束前，舊 commands 不應直接移除；應至少完成一個 release cycle 的
warning、文件與 migration coverage。

### Stage 2 tests

- task 與 workflow 都能透過 mango run 啟動，且輸出相同格式的 run reference。
- mango runs list 能同時顯示 active 與 terminal records。
- status filter、project/target/trigger filters 的結果與 ordering 穩定。
- show 對 active 與 terminal run 都能工作。
- workflow root、child task、attempts 與 retry lineage 的 detail mapping 正確。
- interactive `mango runs` 與 `mango runs list` 使用與 history 相同的階層、分頁、
  refresh、retained-output 操作；terminal detail lazy-load 且 active run 不呼叫
  `history.get`。
- watch timeout、Ctrl-C、daemon unavailable 與 superseded state 行為正確。
- cancel、retry 只允許合法 status，且不會在 ambiguous prefix 下 mutation。
- logs 在 process 尚未結束、已結束、workflow child 與 missing log 的行為一致。
- prune --before、prune --all --yes 的安全確認、terminal-only deletion 與
  restore/backup behavior 維持不變。
- 新舊 CLI 對同一 daemon 的 compatibility matrix smoke tests。
- JSON schema golden tests；不能混用舊 history fields 與新 run fields 而沒有明確
  compatibility mapping。

### Stage 2 documentation

- 將 task/workflow examples 改為 mango run task 與 mango run workflow。
- 新增 Run lifecycle 章節，解釋 queued、running、terminal、retry 與 run reference，
  不使用 execution/history storage terminology 作為入門前置知識。
- 將所有 execution／history 範例移到 migration 或 advanced reference。
- 文件說明 mango runs 的 default scope、filter、ordering、retention 與 prune safety。
- 更新 release compatibility table，包含舊 command、replacement、行為差異與
  預計移除版本。

### Stage 2 acceptance criteria

- 新手只需記住 run 啟動工作、runs 查詢與控制工作。
- active 與 terminal run 不再需要不同的 top-level user mental model。
- mango runs show REF 可取代 execution get 與 history show。
- retry、cancel、watch、logs、prune 都以同一套 run reference 語意運作。
- 舊命令仍可執行，並提供正確 canonical replacement。
- 新 facade 不會造成資料重複、history 遺失或 run ID lineage 斷裂。

---

## Stage 3: Major-release Cleanup and Removal

### Goal

在至少一個相容週期後，移除重複或只暴露內部模型的舊入口，讓預設 help 真正反映
canonical CLI，而不是永久堆積 aliases。

### Removal policy

刪除命令前必須同時滿足：

- 已在至少一個 minor release 中標示 deprecated。
- README、migration guide、release notes 與 completion 都已改用 replacement。
- 主要 repository examples、CI scripts、PowerShell scripts 與 smoke tests 不再依賴舊命令。
- 有實際使用資料或明確產品決策支持移除；不能只因為命令看起來不漂亮就刪除。
- 舊命令失敗時仍提供清楚的 replacement，至少保留一個 major release 的 migration
  error path，或以 compatibility binary/flag 提供過渡。

### Commands to remove or hide

第一優先：

- root ps 與 root ls 的重複入口。
- task ls、workflow ls、schedule ls、project ls 的 primary help entries；
  canonical 形式改用 list。
- execution namespace。
- history namespace。

第二優先：

- root enable／disable，改由 service enable/disable 使用。
- 不必要的 project add，一般流程改用 up PATH；若仍需要 registry-only
  operation，命名為 project register 並保留在 advanced reference。
- mangod run 不放在一般使用者 quick-start；保留給 packaging、service manager、
  foreground debugging 與開發者。

建議永久保留的高頻 shortcuts：

    mango up
    mango down
    mango status
    mango logs
    mango start
    mango stop
    mango restart
    mango run
    mango runs

### Final root help target

最終 root help 的主要入口目標為 10–12 個：

    Start here:
      init
      up
      down
      status
      logs
      run
      runs

    Manage:
      service
      project
      schedule

    Advanced:
      config
      daemon
      doctor

events、monitor、startup 可作為 Advanced commands 保留，或在不破壞可發現性的
前提下移至 daemon／system namespace。無論採哪種形式，都不得再與核心入口混在同一層
而沒有分類。

### Versioning and compatibility

- 在 major release notes 中列出所有移除 command 與 replacement。
- CLI command removal 與 runs facade 的 semantic cutover 應在同一個明確版本邊界
  進行，避免使用者同時面對多個半完成的 command model。
- 保留 daemon-side old IPC methods 一段時間，讓 CLI upgrade 與 daemon upgrade 不必
  完全同時發生；若 protocol contract 需要 major bump，依既有 compatibility policy
  fail closed。
- 不自動刪除舊 registry、history、logs 或 generation state。
- 如果 release 後發現新 command model 有重大問題，提供：

  - 舊 command compatibility flag 或 compatibility binary。
  - 舊 IPC method fallback。
  - 清楚的 rollback 文件。

### Stage 3 tests

- Default root help snapshot 只包含 canonical commands 與正確分組。
- 每個移除 command 都有 migration error test。
- 舊 command 不會被誤解析成另一個 command 或靜默執行不同操作。
- completion 不再產生 removed commands。
- packaged binary、Windows PowerShell、Unix shell smoke tests 使用 canonical syntax。
- 新舊 CLI／daemon matrix 測試確認 protocol mismatch 會明確失敗，不會靜默解讀錯誤。
- 升級、降級、rollback 後的 project registry、history、logs、run references 與
  generation snapshots 保持可讀。

### Stage 3 documentation

- README 只保留 canonical quick start 與 canonical CLI reference。
- 舊命令集中到 docs/migration-cli.md，每一條包含：舊 syntax、新 syntax、行為差異、
  版本邊界與 rollback 方式。
- 更新 plans/README.md 的 plan status 與 dependency flow。
- 更新 release checklist，加入 root help snapshot、completion、old-command migration
  error、JSON compatibility 與 packaged smoke tests。
- 所有錯誤訊息、README、examples、PowerShell scripts 與 release docs 使用同一套命名。

### Stage 3 acceptance criteria

- Default help 不再暴露重複入口或 execution/history storage terminology。
- 核心日常流程最多需要理解七個概念：init、up、status、logs、run、runs、down。
- 舊命令移除後，錯誤訊息仍能引導使用者完成遷移。
- 所有現有資料與 run lineage 維持可讀、可查詢、可 rollback。
- CLI reference、completion、examples、release docs 與實作完全一致。

## Cross-stage quality gate

每個階段都必須通過：

    go test ./...
    go test -race ./...
    go vet ./...
    go build ./...
    cargo fmt --manifest-path mango-shim/Cargo.toml -- --check
    cargo test --manifest-path mango-shim/Cargo.toml
    cargo clippy --manifest-path mango-shim/Cargo.toml --all-targets -- -D warnings

CLI-specific checks：

- root and nested help snapshot tests。
- canonical/legacy command mapping tests。
- JSON stdout purity tests。
- exit status and stderr warning tests。
- target resolution and ambiguity tests。
- daemon unavailable、auto-start、timeout、Ctrl-C 與 stale endpoint tests。
- Windows、Linux、macOS command syntax and executable discovery tests。
- compatibility matrix tests for old/new CLI and old/new daemon combinations。

## Rollout checklist

### Before Stage 1 release

- [x] Canonical command specification review complete。
- [x] Target grammar and ambiguity rules documented。
- [x] Root help groups and examples approved。
- [x] --json support matrix finalized。
- [x] Deprecation policy and warning format finalized。
- [x] README schema version contradiction fixed。

### Before Stage 2 release

- [x] mango run task/workflow available。
- [x] mango runs CLI facade available。
- [x] Active/terminal ordering and filters covered by tests。
- [x] Retry lineage and run reference behavior verified。
- [x] Old execution/history commands emit migration guidance。
- [x] JSON schema and release compatibility docs updated。

### Before Stage 3 major release

- [x] Repository and package examples use canonical commands only。
- [x] Root help contains no duplicate primary entries。
- [x] Removed command migration errors are tested。
- [ ] Completion and all platform smoke tests are updated。
- [ ] Upgrade, rollback and compatibility matrix pass。
- [ ] Release notes contain a complete CLI migration table。

## Success metrics

如果 Mango 未建立 command telemetry，先以文件 review、新手 usability test、CI
command audit 與 support issue 分類作為替代資料。目標為：

- 預設 root help 的主要入口不超過 12 個。
- init 到第一個 ready service 不需要使用者手動管理 daemon。
- 日常 service 操作不需要知道 ps 與 ls 的差異。
- task/workflow 執行不需要知道 execution 與 history 的差異。
- 每個概念只有一個文件中的 canonical syntax。
- 非互動 commands 的 JSON 行為可預測且不含 warning/progress 污染。
- 主要文件、shell completion、examples 與實際 Cobra tree 保持一致。

## Out of scope

- 不引入遠端控制平面或 multi-host orchestration。
- 不修改 Mango YAML schema 來配合 CLI 命名。
- 不因 CLI 移除而自動刪除 registry、history、logs 或 snapshots。
- 不在沒有 ambiguity rules 的情況下加入完全隱式的 target inference。
- 不把所有進階能力藏到一個無語意的 admin 命令，導致維運功能反而不可發現。
