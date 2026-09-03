# goserve

goserve 是使用 Go 開發的跨平台 CLI process manager，透過背景 daemon 管理任何可執行檔，不限定 Node.js。

支援 Windows、Linux 與 macOS，主要功能包括：

- 多 project、多 process 管理
- process 啟動、停止、重啟與 crash-loop 保護
- stdout／stderr 日誌與大小輪替
- CPU、RSS memory、uptime 監控
- 五欄位 cron 排程與 IANA timezone
- Unix Domain Socket／Windows Named Pipe 本機 IPC
- Windows Task Scheduler、Linux systemd user、macOS launchd 每使用者開機自啟
- 互動式 goserve monitor

## 安裝與建置

直接執行：

~~~powershell
go run ./cmd/goserve --help
~~~

建置可執行檔：

~~~powershell
New-Item -ItemType Directory -Force bin
go build -o bin/goserve.exe ./cmd/goserve
~~~

Linux／macOS：

~~~bash
mkdir -p bin
go build -o bin/goserve ./cmd/goserve
~~~

測試與靜態檢查：

~~~powershell
go test ./...
go vet ./...
~~~

## 資料目錄

預設使用作業系統的 user config／cache 目錄：

- Windows：%APPDATA%\goserve 與 %LOCALAPPDATA%\goserve
- Linux：$XDG_CONFIG_HOME/goserve、$XDG_CACHE_HOME/goserve 或 user home fallback
- macOS：~/Library/Application Support/goserve 與 user cache 目錄

測試或需要隔離環境時，可以設定 GOSERVE_HOME：

~~~powershell
$env:GOSERVE_HOME = "C:\temp\goserve-test"
~~~

此時 registry、runtime、logs、state 都會放在該目錄下。

主要內容：

~~~text
GOSERVE_HOME/
├─ projects.json
├─ runtime/
│  ├─ daemon.pid
│  └─ goserve.sock       # Unix；Windows 使用 Named Pipe
├─ logs/
│  └─ <project>/<process>/
└─ state/
~~~

## 快速開始

### 1. 驗證設定並註冊 project

~~~powershell
goserve config validate .\goserve.example.toml
goserve project add .\goserve.example.toml
~~~

project add 會把 TOML 的絕對路徑寫入 project registry。設定檔仍由使用者自行管理，goserve 不會覆蓋原始 TOML。

### 2. 啟動 daemon

前景執行，適合開發與除錯：

~~~powershell
goserve daemon run
~~~

背景執行：

~~~powershell
goserve daemon start
~~~

### 3. 套用設定與查看 process

~~~powershell
goserve project apply demo
goserve list
goserve status demo/api
~~~

### 4. 查看日誌與監控

~~~powershell
goserve logs demo/api --stream all --tail 50
goserve logs demo/api --stream stdout --follow
goserve monitor
~~~

## CLI 指令總覽

一般語法：

~~~text
goserve <command> [subcommand] [arguments] [options]
~~~

所有指令成功時通常回傳 exit code 0；參數錯誤、daemon 無法連線或操作失敗時回傳非零 exit code。

### 全域輸出選項

所有 command 都可以在 command 前或後使用以下選項：

~~~text
--color=auto|always|never
--json
~~~

範例：

~~~powershell
goserve --color=always list
goserve list --color=never
goserve status demo/api --json
~~~

`--color=auto` 是預設值。當 stdout／stderr 連接互動式 TTY 時會啟用顏色；輸出被 pipe、redirect 或執行於 CI 時會自動停用。設定 `NO_COLOR` 環境變數也會停用 auto 模式的顏色；明確指定 `--color=always` 時仍會使用顏色。

`--json` 會將支援 structured output 的指令轉為 JSON，且 JSON 永遠不包含 ANSI 顏色控制碼，適合 CI 與腳本使用。支援的指令包括 `daemon status`、`project list`、`apply`、`list`、`status`、process lifecycle、`schedule`、`startup status` 與 `doctor`。

`logs`、`monitor`、`config validate` 與 startup install/uninstall 維持文字或互動式輸出，不支援 `--json`。

### daemon

管理背景 daemon。

~~~text
goserve daemon run
goserve daemon start
goserve daemon stop
goserve daemon restart
goserve daemon status
~~~

| 指令 | 說明 |
| --- | --- |
| run | 在目前終端以前景模式啟動 daemon，按 Ctrl+C 停止。 |
| start | 背景啟動 daemon；若已執行則顯示 already running。 |
| stop | 透過本機 IPC 要求 daemon 停止。 |
| restart | 停止目前 daemon 後重新背景啟動。 |
| status | 顯示 daemon PID、API version 與狀態；daemon 未執行時顯示 stopped。若個別 project 設定無法載入，會顯示 degraded 與 config_errors，但 daemon 仍會繼續服務其他 project。 |

daemon 目前沒有額外參數。

daemon start 會等待本機 IPC health check 成功後才回報啟動成功；若 daemon 在啟動期間失敗，CLI 會回傳錯誤並顯示 daemon log 的最近內容。

人類可讀輸出：

~~~text
Daemon status
FIELD       | VALUE
------------+---------
status      | ok
pid         | 12345
api version | 1
~~~

若需要原始結構化資料：

~~~powershell
goserve daemon status --json
~~~

需要 daemon 提供服務的指令（例如 `list`、`status`、process action 與 `logs`）若無法連線，會提示：

~~~text
daemon is not running; start it with: goserve daemon start
~~~

請先啟動 daemon，再執行這些指令。`--follow` 只適用於 `logs`，例如：

~~~powershell
goserve logs demo/api --follow
~~~

`status`、`start`、`stop`、`restart`、`enable`、`disable` 與 `logs` 的目標參數可使用 `PROJECT/PROCESS` 或 `ID`；`logs` 另外支援 `PROJECT/SCHEDULE`。id 可從 `goserve list` 或 `goserve status PROJECT/PROCESS` 取得，例如 `goserve stop 2`。

### project

註冊、移除與列出 project。

#### project add

~~~text
goserve project add PATH
~~~

參數：

| 參數 | 必填 | 預設值 | 說明 |
| --- | --- | --- | --- |
| PATH | 是 | 無 | TOML 設定檔路徑，可使用相對路徑。project 名稱取自 TOML 的 `project` 欄位。 |

project name 只能使用英數字、.、_、-，且第一個字元必須是英數字。

範例：

~~~powershell
goserve project add .\goserve.example.toml
goserve project add C:\apps\demo\goserve.toml
~~~

#### project remove

~~~text
goserve project remove NAME
~~~

| 參數 | 必填 | 說明 |
| --- | --- | --- |
| NAME | 是 | 要從 registry 移除的 project 名稱。 |

此指令不會刪除 TOML、日誌或應用程式檔案。

#### project list

~~~text
goserve project list
~~~

列出 registry 中的 project、設定檔路徑、啟用狀態與最後套用時間，需要 daemon 執行。

使用 `goserve project list --json` 可輸出 JSON。

#### project apply

重新讀取 project TOML 並套用變更。

~~~text
goserve project apply NAME
~~~

| 參數 | 必填 | 說明 |
| --- | --- | --- |
| NAME | 是 | 已註冊的 project 名稱。 |

設定驗證失敗時，不應套用該次設定。新增 process 會依 autostart 決定是否啟動；設定變更或刪除 process 時，舊 process 會被停止。

### config validate

只解析與驗證 TOML，不啟動或停止 process。

~~~text
goserve config validate PATH
~~~

| 參數 | 必填 | 說明 |
| --- | --- | --- |
| PATH | 是 | 要驗證的 TOML 設定檔。 |

驗證項目包括：

- version 是否為目前支援的版本 1
- project、process、schedule 名稱是否有效且不重複
- command 是否有指定；實際 executable 是否可啟動會在 process start 時檢查
- restart、duration、log size 設定格式
- cron 表達式與 timezone
- schedule action 與 target

### list

列出所有 project 的 process 與全域 process id。每次 daemon 啟動或重啟時，process id 會重新從 0 開始分配；daemon 執行期間重新 apply 則會保留現有 process 的 id。

~~~text
goserve list
~~~

顯示：

- process id
- project/process
- state
- PID
- listening TCP／UDP ports
- CPU percentage
- RSS memory
- memory percentage
- restart count

預設使用彩色表格；可用 `goserve list --color=never` 取得不含顏色的穩定文字輸出，或使用 `goserve list --json` 取得 JSON。

### status

查看單一 process 的完整狀態。

~~~text
goserve status PROJECT/PROCESS|ID
~~~

必要參數：

| 參數 | 說明 |
| --- | --- |
| PROJECT/PROCESS\|ID | process key（例如 demo/api）或全域非負整數 id。 |

輸出還包含啟動時間、uptime、最後退出碼、最後錯誤、command line、stdout／stderr 日誌路徑與 disabled 狀態。

`status` 預設輸出 key/value detail table；使用 `goserve status PROJECT/PROCESS|ID --json` 可取得原始 JSON。

### process lifecycle

以下指令都使用相同語法：

~~~text
goserve start PROJECT/PROCESS|ID
goserve stop PROJECT/PROCESS|ID
goserve restart PROJECT/PROCESS|ID
goserve enable PROJECT/PROCESS|ID
goserve disable PROJECT/PROCESS|ID
~~~

| 指令 | 說明 |
| --- | --- |
| start | 啟動 process，並清除本次 crash-loop 計數。 |
| stop | 暫時停止 process，不修改 TOML 的 autostart。 |
| restart | 先停止再啟動 process。 |
| enable | 清除 disabled 狀態並啟動 process。 |
| disable | 設為 disabled 並停止 process；直到 enable 或重新套用設定前不會 autostart。 |

process lifecycle 成功時預設輸出簡短訊息，例如 `Process demo/api stopped`；使用 id 操作時仍會輸出 canonical key。加上 `--json` 可保留結構化結果。

Unix 會先對 process group 發送 SIGTERM，逾時後強制終止。Windows 使用 Job Object 管理 process tree；由於 Go 的 `os.Process.Signal(os.Interrupt)` 不支援 Windows，stop 會立即終止 Job Object，若 Job Object 無法建立才以 `taskkill /T /F` 作為 fallback，避免無效等待 stop timeout。

### logs

查看 process 的 stdout／stderr。

~~~text
goserve logs TARGET [--stream STREAM] [--tail N] [--follow]
goserve logs clear TARGET
~~~

參數：

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| TARGET | 無 | process key、schedule key 或全域整數 process id。 |
| --stream | stdout | 可選 stdout、stderr 或 all。 |
| --tail | 100 | 顯示最後幾行；必須是整數。 |
| --follow | false | 持續追蹤新增內容，按 Ctrl+C 結束。 |

`goserve logs clear TARGET` 會清除指定 process 或 schedule 的 stdout／stderr
目前日誌與所有輪替檔。若 process 仍在執行，會保留開啟中的 writer，清除後的
新輸出仍會繼續寫入；沒有日誌檔時視為成功。

日誌位置：

~~~text
<logs>/<project>/<process>/stdout.log
<logs>/<project>/<process>/stderr.log
~~~

預設單檔大小為 100MiB，保留 10 個輪替檔；可在 TOML 的 defaults 或 process 欄位調整。

### monitor

啟動互動式終端監控：

~~~text
goserve monitor
~~~

主畫面每秒更新 process 狀態、PID、CPU、RSS、memory、uptime 與 restart count。

| 按鍵 | 操作 |
| --- | --- |
| ↑／↓ | 上下選取 process。 |
| j／k | 上下選取 process 的替代按鍵。 |
| s | stop。 |
| r | restart。 |
| e | enable。 |
| d | disable。 |
| l | 查看選取 process 的最近日誌。 |
| Enter | 查看選取 process 的詳細資訊。 |
| q | 離開 monitor。 |

若 stdout 或 stdin 不是 TTY，monitor 會退化成輸出一次 process table。

### schedule

查看、手動執行排程與查詢排程歷史。

~~~text
goserve schedule list
goserve schedule history
goserve schedule run PROJECT/SCHEDULE
~~~

| 指令 | 說明 |
| --- | --- |
| list | 列出 schedule、cron、timezone、action 與 concurrency。 |
| history | 顯示本次 daemon 執行期間的排程紀錄。 |
| run | 立即執行指定 schedule，不等待下一次 cron 時間。 |

schedule run 的 key 格式為 PROJECT/SCHEDULE。

`action = "run"` 的 schedule 可直接使用 schedule key 查看日誌：

~~~powershell
goserve logs PROJECT/SCHEDULE --stream all
goserve logs PROJECT/SCHEDULE --stream all --follow
~~~

例如 `goserve logs demo/nightly-job --stream all`。daemon 會將它解析至 `schedule-nightly-job` 日誌目錄；`start`、`stop` 與 `restart` 類型的 schedule 沒有自己的 command logs，請查看目標 process 的日誌。

### startup

管理每使用者開機自啟：

~~~text
goserve startup install
goserve startup uninstall
goserve startup status
~~~

| 指令 | Windows | Linux | macOS |
| --- | --- | --- | --- |
| install | User-level Task Scheduler | systemd --user | launchd LaunchAgent |
| uninstall | 移除 goserve task | 停用並移除 user service | unload 並移除 LaunchAgent |
| status | 查詢 task | 檢查 user service 檔案 | 檢查 plist 檔案 |

startup service 只會啟動 daemon；process 是否啟動仍由 TOML 的 autostart 決定。

### doctor

檢查目前環境：

~~~text
goserve doctor
~~~

會顯示作業系統、goserve root、registry 路徑、daemon 是否可連線與 startup 是否已安裝。

### help

~~~text
goserve help
goserve --help
~~~

顯示 CLI 指令總覽。

## TOML 設定參考

完整範例請參考 [goserve.example.toml](goserve.example.toml)。

### 根欄位

~~~toml
version = 1
project = "demo"
~~~

| 欄位 | 必填 | 說明 |
| --- | --- | --- |
| version | 是 | 目前必須為 1。 |
| project | 是 | project 名稱，也必須與 registry 名稱一致。 |

### defaults

~~~toml
[defaults]
working_dir = "."
restart = "on-failure"
stop_timeout = "10s"
log_max_size = "100MiB"
log_max_files = 10
metrics_interval = "1s"
max_restarts = 10
restart_window = "5m"
stable_after = "1m"
inherit_env = true
~~~

| 欄位 | 預設值 | 說明 |
| --- | --- | --- |
| working_dir | . | 相對 TOML 所在目錄解析。 |
| restart | on-failure | 可選 never、on-failure、always。 |
| stop_timeout | 10s | graceful stop 等待時間，逾時後強制終止。 |
| log_max_size | 100MiB | 單一 stdout／stderr 日誌檔的最大大小。 |
| log_max_files | 10 | 輪替檔數量。 |
| metrics_interval | 1s | metrics 取樣間隔。 |
| max_restarts | 10 | restart window 內允許的最大重啟次數；0 會使用預設值。 |
| restart_window | 5m | crash-loop 計數時間窗。 |
| stable_after | 1m | 穩定執行多久後清除 restart 計數。 |
| inherit_env | true | 是否繼承 daemon 的環境變數。 |

duration 使用 Go duration 格式，例如 500ms、10s、5m、1h。

size 支援 bytes、KB／MB／GB 與 KiB／MiB／GiB，例如 100MiB。

### processes

~~~toml
[[processes]]
name = "api"
command = "go"
args = ["run", "./examples/api", "--port", "8080"]
working_dir = "."
autostart = true
restart = "always"
stop_timeout = "10s"

[processes.env]
APP_ENV = "development"
~~~

| 欄位 | 必填 | 預設值 | 說明 |
| --- | --- | --- | --- |
| name | 是 | 無 | project 內唯一的 process 名稱。 |
| command | 是 | 無 | executable 名稱或路徑，不經 shell。 |
| args | 否 | [] | 傳給 executable 的參數陣列。 |
| working_dir | 否 | defaults 值 | process 的工作目錄。 |
| env | 否 | 繼承環境 | 要覆寫或新增的環境變數。 |
| autostart | 否 | false | daemon 啟動或 apply 後是否自動啟動。 |
| restart | 否 | defaults 值 | never、on-failure 或 always。 |
| stop_timeout | 否 | defaults 值 | graceful stop timeout。 |
| max_restarts | 否 | defaults 值 | crash-loop 重啟上限。 |
| restart_window | 否 | defaults 值 | crash-loop 計數時間窗。 |
| stable_after | 否 | defaults 值 | 清除 crash counter 的穩定時間。 |

command 執行規則：

- 不支援管線、重導向、&& 等 shell 語法。
- command 中含 / 或 \ 時，會依 process working directory 解析相對路徑。
- 不含路徑分隔符時，會從作業系統 PATH 尋找 executable。
- 若需要 shell，請把 shell 本身當成 command，例如 Windows 使用 cmd.exe，Unix 使用 sh，並自行在 args 中傳入參數。

### schedules

~~~toml
[[schedules]]
name = "nightly-job"
cron = "0 2 * * *"
timezone = "Asia/Taipei"
action = "run"
command = "go"
args = ["run", "./examples/one-task", "--iterations", "3"]
concurrency = "forbid"
~~~

| 欄位 | 必填 | 預設值 | 說明 |
| --- | --- | --- | --- |
| name | 是 | 無 | project 內唯一的 schedule 名稱。 |
| cron | 是 | 無 | 五欄位 cron：分、時、日、月、星期。 |
| timezone | 否 | Local | IANA timezone，例如 Asia/Taipei、UTC。 |
| action | 是 | 無 | run、start、stop 或 restart。 |
| target | action 非 run 時必填 | 無 | 要操作的 process name。 |
| command | action=run 時必填 | 無 | 一次性 task executable。 |
| args | 否 | [] | 一次性 task 的參數陣列。 |
| working_dir | 否 | defaults 值 | task 工作目錄。 |
| env | 否 | 繼承環境 | task 環境變數。 |
| concurrency | 否 | forbid | 可選 forbid 或 allow。 |

排程規則：

- daemon 離線期間錯過的排程不補執行。
- forbid 會跳過上一個相同 schedule 尚未完成的執行。
- cron 與 timezone 錯誤會使 config validate／apply 失敗。
- schedule task 日誌會寫入 process log root 下的 schedule-<name> 目錄。

## Process 狀態與重啟

可能的 process state：

~~~text
stopped
starting
running
stopping
exited
backing_off
crash_loop
failed
disabled
unknown
~~~

restart policy：

- never：退出後保持 exited。
- on-failure：只有非零退出碼或異常退出才重啟。
- always：正常退出與異常退出都重啟。

自動重啟使用 exponential backoff，初始約 1 秒，最大 60 秒。超過 restart limit 後進入 crash_loop，人工 restart 或 start 可重新嘗試。

## 測試程式

### API server

原始碼：[examples/api/main.go](examples/api/main.go)

~~~powershell
go run ./examples/api --port 8080 --interval 3s
~~~

參數：

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| --port | 8080 | HTTP listen port，範圍為 1～65535。 |
| --interval | 5s | heartbeat 輸出間隔。 |

endpoint：

~~~text
GET /
GET /error
~~~

- GET /：回傳成功訊息並寫入 stdout request log。
- GET /error：回傳 HTTP 500，並寫入 stderr。
- 每次 heartbeat 會寫入 stdout；每三次 heartbeat 會額外寫入 stderr。
- 收到 Ctrl+C／SIGTERM 時會 graceful shutdown。

測試：

~~~powershell
Invoke-WebRequest http://127.0.0.1:8080/
Invoke-WebRequest http://127.0.0.1:8080/error
~~~

### one-task

原始碼：[examples/one-task/main.go](examples/one-task/main.go)

~~~powershell
go run ./examples/one-task --iterations 5 --interval 500ms
~~~

參數：

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| --iterations | 5 | 執行次數，必須至少為 1。 |
| --interval | 500ms | 每次 iteration 間的等待時間。 |
| --fail | false | 完成輸出後以 exit code 2 結束。 |

輸出行為：

- 啟動資訊與完成資訊寫入 stdout。
- task diagnostic 與奇數 iteration 訊息寫入 stderr。
- --fail 可測試 on-failure／always restart policy 與非零退出碼。

## 常見操作流程

啟動 API 與註冊 task：

~~~powershell
goserve project add .\goserve.example.toml
goserve daemon start
goserve project apply demo
goserve list
~~~

執行一次性 task：

~~~powershell
goserve start demo/one-task
goserve logs demo/one-task --stream all --follow
~~~

測試排程 task：

~~~powershell
goserve schedule list
goserve schedule run demo/nightly-job
goserve schedule history
~~~

開機自啟：

~~~powershell
goserve startup install
goserve startup status
~~~

## 開發與驗證

~~~powershell
go test ./...
go test -race ./internal/...
go vet ./...
~~~

跨平台 build：

~~~powershell
$env:GOOS = "windows"; $env:GOARCH = "amd64"; go build -o bin/goserve-windows-amd64.exe ./cmd/goserve
$env:GOOS = "linux";   $env:GOARCH = "amd64"; go build -o bin/goserve-linux-amd64 ./cmd/goserve
$env:GOOS = "darwin";  $env:GOARCH = "arm64"; go build -o bin/goserve-darwin-arm64 ./cmd/goserve
~~~
