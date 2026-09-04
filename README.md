# Mango

mango 是使用 Go 開發的跨平台 service manager：`mango` CLI 透過 `mangod` 背景 daemon 管理任何可執行檔，不限定 Node.js。

支援 Windows、Linux 與 macOS，主要功能包括：

- 多 project、多 service 管理
- service 啟動、停止、重啟與 crash-loop 保護
- stdout／stderr 日誌與大小輪替
- CPU、RSS memory、uptime 監控
- 五欄位 cron 排程與 IANA timezone
- Unix Domain Socket／Windows Named Pipe 本機 IPC
- Windows Task Scheduler、Linux systemd user、macOS launchd 每使用者開機自啟
- 互動式 mango monitor

## 安裝與建置

直接執行：

~~~powershell
go run ./cmd/mango --help
~~~

建置可執行檔：

~~~powershell
New-Item -ItemType Directory -Force bin
go build -o bin/mango.exe ./cmd/mango
go build -o bin/mangod.exe ./cmd/mangod
~~~

Linux／macOS：

~~~bash
mkdir -p bin
go build -o bin/mango ./cmd/mango
go build -o bin/mangod ./cmd/mangod

正式安裝時請同時提供兩個 binary，並放在同一個目錄，讓 `mango daemon start` 能優先找到相同版本的 `mangod`。
~~~

測試與靜態檢查：

~~~powershell
go test ./...
go vet ./...
~~~

## 資料目錄

預設使用作業系統的 user config／cache 目錄：

- Windows：%APPDATA%\mango 與 %LOCALAPPDATA%\mango
- Linux：$XDG_CONFIG_HOME/mango、$XDG_CACHE_HOME/mango 或 user home fallback
- macOS：~/Library/Application Support/mango 與 user cache 目錄

測試或需要隔離環境時，可以設定 MANGO_HOME：

~~~powershell
$env:MANGO_HOME = "C:\temp\mango-test"
~~~

此時 registry、runtime、logs、state 都會放在該目錄下。

主要內容：

~~~text
MANGO_HOME/
├─ projects.json
├─ daemon.yaml           # daemon 全域設定（可選）
├─ runtime/
│  ├─ daemon.pid
│  └─ mango.sock       # Unix；Windows 使用 Named Pipe
├─ logs/
│  └─ <project>/<service>/
└─ state/
	└─ schedule-history.json
~~~

## 快速開始

### 1. 驗證設定並註冊 project

~~~powershell
mango config validate .\mango.example.yaml
mango project add demo .\mango.example.yaml
~~~

project add 會把 project name 與 YAML 的絕對路徑寫入 project registry。設定檔仍由使用者自行管理，mango 不會覆蓋原始 YAML。
若 registry 已有相同 project name，`project add` 會拒絕此次加入並保留既有註冊內容；如需改用其他設定檔，請先執行 `mango project remove NAME`。

設定檔目前僅支援 `.yaml`。舊 `.toml` 設定不會自動轉換；請手動轉換後重新執行 `project remove` 與 `project add`。

### 2. 啟動 daemon

前景執行，適合開發與除錯：

~~~powershell
mangod run
~~~

背景執行：

~~~powershell
mango daemon start
~~~

### 3. 套用設定與查看 service

~~~powershell
mango project apply demo
mango ls
mango status demo/api
~~~

### 4. 查看日誌與監控

~~~powershell
mango logs demo/api --stream all --tail 50
mango logs demo/api --stream stdout --follow
mango logs demo/api demo/nightly-job 2 --stream all
mango monitor
~~~

## CLI 指令總覽

一般語法：

~~~text
mango <command> [subcommand] [arguments] [options]
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
mango --color=always ls
mango ls --color=never
mango status demo/api --json
~~~

`--color=auto` 是預設值。當 stdout／stderr 連接互動式 TTY 時會啟用顏色；輸出被 pipe、redirect 或執行於 CI 時會自動停用。設定 `NO_COLOR` 環境變數也會停用 auto 模式的顏色；明確指定 `--color=always` 時仍會使用顏色。

`--json` 會將支援 structured output 的指令轉為 JSON，且 JSON 永遠不包含 ANSI 顏色控制碼，適合 CI 與腳本使用。支援的指令包括 `daemon status`、`project ls`、`apply`、`ls`、`status`、service lifecycle、`schedule`、`startup status` 與 `doctor`。

`logs`、`monitor`、`config validate` 與 startup install/uninstall 維持文字或互動式輸出，不支援 `--json`。

### daemon

管理背景 daemon。

~~~text
mangod run
mango daemon start
mango daemon stop
mango daemon restart
mango daemon status
mango daemon logs [--tail 50]
mango daemon logs --follow
~~~

| 指令 | 說明 |
| --- | --- |
| `mangod run` | 在目前終端以前景模式啟動 daemon，按 Ctrl+C 停止。 |
| start | 背景啟動 daemon；若已執行則顯示 already running。 |
| stop | 透過本機 IPC 要求 daemon 停止。 |
| restart | 停止目前 daemon，等待 IPC endpoint 關閉後重新背景啟動。 |
| status | 顯示 daemon PID、API version 與狀態；daemon 未執行時顯示 stopped。若個別 project 設定無法載入，會顯示 degraded 與 config_errors，但 daemon 仍會繼續服務其他 project。 |
| logs | 直接讀取本機 `daemon.log`；daemon 未執行時仍可查看既有內容。 |

`mangod` 目前只有 `run` 入口，沒有其他參數。`mango daemon` 負責控制、查詢 daemon，以及直接查看本機 daemon log。

`mango daemon logs` 預設顯示最後 15 行；`--tail 0` 顯示完整 `daemon.log`，`--follow` 先顯示目前內容後追蹤新增內容。daemon.log 不存在時視為空 log；每行以 `｜daemon｜` 前綴輸出，互動式 TTY 下前綴為青色。

daemon IPC 的 service lifecycle method 為 `service.ls`、`service.get`、`service.start`、`service.stop`、`service.restart`、`service.enable`、`service.disable` 與 `service.bulk`；舊的 `process.*` method 不再接受。

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
mango daemon status --json
~~~

需要 daemon 提供服務的指令（例如 `list`、`status`、service action 與 service `logs`）若無法連線，會提示：

~~~text
daemon is not running; start it with: mango daemon start
~~~

請先啟動 daemon，再執行上述需要 IPC 的指令。`mango daemon logs` 直接讀取本機檔案，不需要 daemon 執行；`--follow` 可用於兩種 `logs` 指令，例如：

~~~powershell
mango logs demo/api --follow
~~~

`status`、`start`、`stop`、`restart`、`enable`、`disable` 與 `logs` 的目標參數可使用 `PROJECT/SERVICE` 或 `ID`；service lifecycle 另外支援直接指定 `PROJECT` 以操作該 project 的全部 service，`logs` 另外支援 `PROJECT/SCHEDULE`。id 可從 `mango ls` 或 `mango status PROJECT/SERVICE` 取得，例如 `mango stop 2`。

### project

註冊、移除與列出 project。

#### project add

~~~text
mango project add NAME PATH
~~~

參數：

| 參數 | 必填 | 預設值 | 說明 |
| --- | --- | --- | --- |
| NAME | 是 | 無 | 要註冊的 project 名稱。 |
| PATH | 是 | 無 | YAML 設定檔路徑，可使用相對路徑。 |

project name 必須以英文字母開頭，後續只能使用英數字、.、_、-；數字開頭的 project name 不允許，裸數字 target 保留給 service ID。

範例：

~~~powershell
mango project add demo .\mango.example.yaml
mango project add demo C:\apps\demo\mango.yaml
~~~

#### project remove

~~~text
mango project remove NAME
~~~

| 參數 | 必填 | 說明 |
| --- | --- | --- |
| NAME | 是 | 要從 registry 移除的 project 名稱。 |

此指令不會刪除 YAML、日誌或應用程式檔案。

#### project rename

~~~text
mango project rename OLD NEW
~~~

以 OLD 的設定檔路徑重新註冊為 NEW，效果等同於 `project remove OLD` 後再 `project add NEW PATH`。YAML 不會被修改；daemon 執行中時會重新載入 registry。

#### project ls

~~~text
mango project ls
~~~

列出 registry 中的 project、設定檔路徑、啟用狀態與最後套用時間，需要 daemon 執行。

使用 `mango project ls --json` 可輸出 JSON。

#### project apply

重新讀取 project YAML 並套用變更。

~~~text
mango project apply NAME
~~~

| 參數 | 必填 | 說明 |
| --- | --- | --- |
| NAME | 是 | 已註冊的 project 名稱。 |

設定驗證失敗時，不應套用該次設定。新增 service 會依 autostart 決定是否啟動；設定變更或刪除 service 時，舊 service 會被停止。

### config validate

只解析與驗證 YAML，不啟動或停止 service。

~~~text
mango config validate PATH
~~~

| 參數 | 必填 | 說明 |
| --- | --- | --- |
| PATH | 是 | 要驗證的 YAML 設定檔。 |

驗證項目包括：

- version 是否為目前支援的版本 2
- project、service、schedule 名稱是否有效且不重複
- command 是否有指定；實際 executable 是否可啟動會在 service start 時檢查
- restart、duration、log size 設定格式
- healthcheck 與 depends_on 的條件、未知服務及循環依賴
- cron 表達式與 timezone
- schedule action 與 target

### ls

列出所有 project 的 service 與全域 service id。每次 daemon 啟動或重啟時，id 會重新從 0 開始分配；daemon 執行期間重新 apply 則會保留現有 service 的 id。

~~~text
mango ls
~~~

顯示：

- service id
- `SERVICE`：project/service，也就是 YAML 定義的 managed service
- `PROCESS`：root process 的作業系統程序名稱
- mango lifecycle state
- health
- root PID 的 OS state
- PID
- listening TCP／UDP ports
- CPU percentage
- RSS memory
- memory percentage
- restart count

若 service 產生子 process，`ls` 會以縮排階層列出所有 descendants。父列的 `SERVICE` 顯示 managed service key，子列的 `SERVICE` 顯示 `-`；`PROCESS` 則分別顯示 root 與 descendant 的作業系統程序名稱。子列顯示子 process 的 PID、OS state、port、CPU、RSS 與 memory；子 process 不會分配 mango service id，也不能直接執行 lifecycle 操作。service 的 PORTS 會彙總 root 與 descendants 的 listening ports。`--json` 會在 managed service 的 `Children` 欄位保留巢狀結構。

`STATE` 是 mango lifecycle，`HEALTH` 是明確設定的 healthcheck 結果，`OS STATE` 是 root PID 的作業系統狀態，三者彼此獨立。未設定 healthcheck 時 HEALTH 顯示 `-`，JSON 為 `null`。

文字表格欄位順序為：`ID | SERVICE | PROCESS | STATE | HEALTH | OS STATE | PID | PORTS | CPU% | RSS | MEM% | RESTART`。

預設使用彩色表格；可用 `mango ls --color=never` 取得不含顏色的穩定文字輸出，或使用 `mango ls --json` 取得 JSON。

### status

查看單一 service 的完整狀態。

~~~text
mango status PROJECT/SERVICE|ID
~~~

必要參數：

| 參數 | 說明 |
| --- | --- |
| PROJECT/SERVICE\|ID | service key（例如 demo/api）或全域非負整數 id。 |

輸出還包含啟動時間、uptime、最後退出碼、最後錯誤、command line、stdout／stderr 日誌路徑與 disabled 狀態。

`status` 預設輸出 key/value detail table；使用 `mango status PROJECT/SERVICE|ID --json` 可取得原始 JSON。

### service lifecycle

以下指令都使用相同語法：

~~~text
mango start PROJECT|PROJECT/SERVICE|ID [TARGET ...]
mango stop PROJECT|PROJECT/SERVICE|ID [TARGET ...]
mango restart PROJECT|PROJECT/SERVICE|ID [TARGET ...]
mango enable PROJECT|PROJECT/SERVICE|ID [TARGET ...]
mango disable PROJECT|PROJECT/SERVICE|ID [TARGET ...]
~~~

| 指令 | 說明 |
| --- | --- |
| start | 啟動 service，並清除本次 crash-loop 計數。若 dependency 尚未滿足，會先進入 waiting。 |
| stop | 暫時停止 service，不修改 YAML 的 autostart。 |
| restart | 先停止再啟動 service；depends_on.restart=true 的 dependent 也會依序重啟。 |
| enable | 清除 disabled 狀態並啟動 service。 |
| disable | 設為 disabled 並停止 service；直到 enable 或重新套用設定前不會 autostart。 |

service lifecycle 可一次指定多個 target，例如 `mango stop demo/api 2 other/web`；成功時預設逐筆輸出簡短訊息，例如 `Service demo/api stopped`，使用 id 操作時仍會輸出 canonical key。加上 `--json` 可取得結構化結果；純 service 或 ID target 的單一操作輸出物件，多個 target 輸出陣列；project target 會依展開後的每個 service 輸出結果陣列。

也可以直接指定 project，例如 `mango restart demo`，daemon 會依 dependency order 操作該 project 的全部 service。`start` 與 `enable` 使用 dependency-first 順序；`stop` 與 `disable` 使用 dependent-first 順序；`restart` 先反向停止，再依 dependency-first 順序啟動。批次操作會繼續處理其他 service，最後彙整錯誤並以 non-zero status 結束；`--json` 會回傳每個 service 的結果陣列。

Unix 會先對 service process group 發送 SIGTERM，逾時後強制終止。Windows 使用 Job Object 管理 process tree；由於 Go 的 `os.Process.Signal(os.Interrupt)` 不支援 Windows，stop 會立即終止 Job Object，若 Job Object 無法建立才以 `taskkill /T /F` 作為 fallback，避免無效等待 stop timeout。

### logs

查看 service 的 stdout／stderr。

~~~text
mango logs TARGET [TARGET ...] [--stream STREAM] [--tail N] [--follow]
mango logs clear TARGET
~~~

參數：

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| TARGET | 無 | 一個以上的 service key、schedule key 或全域整數 service id。 |
| --stream | all | 可選 stdout、stderr 或 all。 |
| --tail | 15 | 顯示最後幾行；必須是整數。 |
| --follow | false | 持續追蹤新增內容，按 Ctrl+C 結束。 |

指定多個 target 時，各 target 的 stdout／stderr 會並行追蹤並依事件抵達順序交錯輸出；每個完整輸出行都會使用解析後的 canonical target 前綴，例如
`｜demo/api｜ log content`。互動式 TTY 會將 stdout 前綴顯示為綠色、stderr 前綴顯示為紅色，其他輸出環境不加入 ANSI 色碼。

`mango logs clear TARGET` 會清除指定 service 或 schedule 的 stdout／stderr
目前日誌與所有輪替檔。若 service 仍在執行，會保留開啟中的 writer，清除後的
新輸出仍會繼續寫入；沒有日誌檔時視為成功。

日誌位置：

~~~text
<logs>/<project>/<service>/stdout.log
<logs>/<project>/<service>/stderr.log
~~~

預設單檔大小為 100MiB，保留 10 個輪替檔；可在 YAML 的 defaults 或 service 欄位調整。

### monitor

啟動互動式終端監控：

~~~text
mango monitor
~~~

主畫面每秒更新 service、process、狀態、health、OS state、PID、port、CPU、RSS、memory、uptime 與 restart count；子 process 以唯讀階層列顯示，並使用與 `mango ls` 相同的 `SERVICE`/`PROCESS` 欄位。

| 按鍵 | 操作 |
| --- | --- |
| ↑／↓ | 上下選取 service。 |
| j／k | 上下選取 service 的替代按鍵。 |
| s | stop。 |
| r | restart。 |
| e | enable。 |
| d | disable。 |
| l | 以與 `mango logs TARGET --follow` 相同的格式追蹤選取 service 的日誌；按任意鍵返回。 |
| Enter | 查看選取 service 的詳細資訊。 |
| q | 離開 monitor。 |

若 stdout 或 stdin 不是 TTY，monitor 會退化成輸出一次 service table。

### schedule

查看、手動執行排程與查詢排程歷史。

~~~text
mango schedule ls
mango schedule history [--tail N]
mango schedule run PROJECT/SCHEDULE
~~~

| 指令 | 說明 |
| --- | --- |
| ls | 列出 schedule、cron、timezone、action、concurrency 與執行狀態。 |
| history | 顯示已保存的排程執行紀錄。 |
| run | 立即執行指定 schedule，不等待下一次 cron 時間。 |

`mango schedule ls` 會在基本設定欄位後顯示 `STATUS`、`LAST_RUN`、`NEXT_RUN` 與 `DURATION`：

- `STATUS` 為 `idle`、`running`、`success` 或 `failed`；執行中狀態優先顯示。
- `LAST_RUN` 是最近一次實際執行的開始時間，包含手動執行；`NEXT_RUN` 是下一次 cron 觸發時間。
- `DURATION` 是最近一次執行耗時；執行中則顯示目前已耗時。尚未有值時顯示 `-`。
- 時間依 schedule 的 timezone 顯示。`--json` 會以 RFC3339 時間、秒數 duration 與 `null` 空值回傳這些欄位。

schedule run 的 key 格式為 PROJECT/SCHEDULE。

`mango schedule history --tail N` 只限制本次輸出的筆數，預設為最近 100 筆；`--tail 0` 顯示所有已保留紀錄，不會修改 daemon 的保留設定。

排程 history 會保存於 `state/schedule-history.json`，daemon 重啟後仍可查詢。可在 `daemon.yaml` 設定全域保留筆數：

~~~yaml
schedule_history_limit: 1000
~~~

預設值 `0` 代表不限制。正數只保留最新 N 筆；高頻率排程建議設定正數以控制 state 檔案大小。

`action = "run"` 的 stderr 會以最多 64 KiB 的尾端內容保存於 history；完整 stderr 仍可用 `mango logs PROJECT/SCHEDULE --stream stderr` 查閱。

`action = "run"` 的 schedule 可直接使用 schedule key 查看日誌：

~~~powershell
mango logs PROJECT/SCHEDULE --stream all
mango logs PROJECT/SCHEDULE --stream all --follow
~~~

例如 `mango logs demo/nightly-job --stream all`。daemon 會將它解析至 `schedule-nightly-job` 日誌目錄；`start`、`stop` 與 `restart` 類型的 schedule 沒有自己的 command logs，請查看目標 service 的日誌。

### startup

管理每使用者開機自啟：

~~~text
mango startup install
mango startup uninstall
mango startup status
~~~

| 指令 | Windows | Linux | macOS |
| --- | --- | --- | --- |
| install | User-level Task Scheduler | systemd --user | launchd LaunchAgent |
| uninstall | 移除 mango task | 停用並移除 user service | unload 並移除 LaunchAgent |
| status | 查詢 task | 檢查 user service 檔案 | 檢查 plist 檔案 |

startup service 只會啟動 daemon；service 是否啟動仍由 YAML 的 autostart 決定。

### doctor

檢查目前環境：

~~~text
mango doctor
~~~

會顯示作業系統、mango root、registry、logs 根目錄、daemon.log、schedule history 路徑、daemon 是否可連線與 startup 是否已安裝。

### help

~~~text
mango help
mango --help
~~~

顯示 CLI 指令總覽。

## YAML 設定參考

完整範例請參考 [mango.example.yaml](mango.example.yaml)。

### 根欄位

~~~yaml
version: 2
~~~

| 欄位 | 必填 | 說明 |
| --- | --- | --- |
| version | 是 | 目前必須為 2。 |

project name 不再寫在 YAML，而是在 `mango project add NAME PATH` 指定。舊設定檔中的 `project:` 欄位會被視為未知欄位，請移除後再重新註冊。

### defaults

~~~yaml
defaults:
  working_dir: "."
  restart: on-failure
  stop_timeout: 10s
  log_max_size: 100MiB
  log_max_files: 10
  metrics_interval: 1s
  max_restarts: 10
  restart_window: 5m
  stable_after: 1m
  inherit_env: true
~~~

| 欄位 | 預設值 | 說明 |
| --- | --- | --- |
| working_dir | . | 相對 YAML 所在目錄解析。 |
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

### services

~~~yaml
services:
  api:
    command: go
    args: [run, ./examples/api, --port, "8080"]
    working_dir: "."
    autostart: true
    restart: always
    stop_timeout: 10s
    environment:
      APP_ENV: development
~~~

| 欄位 | 必填 | 預設值 | 說明 |
| --- | --- | --- | --- |
| service mapping key | 是 | 無 | project 內唯一的 service 名稱。 |
| command | 是 | 無 | executable 名稱或路徑，不經 shell。 |
| args | 否 | [] | 傳給 executable 的參數陣列。 |
| working_dir | 否 | defaults 值 | service 的工作目錄。 |
| environment | 否 | 繼承環境 | 要覆寫或新增的環境變數。 |
| autostart | 否 | false | daemon 啟動或 apply 後是否自動啟動。 |
| restart | 否 | defaults 值 | never、on-failure 或 always。 |
| stop_timeout | 否 | defaults 值 | graceful stop timeout。 |
| max_restarts | 否 | defaults 值 | crash-loop 重啟上限。 |
| restart_window | 否 | defaults 值 | crash-loop 計數時間窗。 |
| stable_after | 否 | defaults 值 | 清除 crash counter 的穩定時間。 |

command 執行規則：

- 不支援管線、重導向、&& 等 shell 語法。
- command 中含 / 或 \ 時，會依 service working directory 解析相對路徑。
- 不含路徑分隔符時，會從作業系統 PATH 尋找 executable。
- 若需要 shell，請把 shell 本身當成 command，例如 Windows 使用 cmd.exe，Unix 使用 sh，並自行在 args 中傳入參數。

### healthcheck 與 depends_on

healthcheck 綁定 service，而不是 process tree。單一 probe 使用 Compose 風格的 `test`：

~~~yaml
services:
  db:
    healthcheck:
      test: [CMD-SHELL, "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
      start_interval: 5s
~~~

腳本若提供多個應用，可使用多個 checks；`policy` 可為 `all`（預設）或 `any`：

~~~yaml
services:
  stack:
    healthcheck:
      policy: all
      interval: 10s
      timeout: 2s
      retries: 3
      checks:
        - test: [CMD, curl, -f, "http://127.0.0.1:8080/health"]
        - test: [CMD, curl, -f, "http://127.0.0.1:9090/metrics"]
~~~

`test` 與 `checks` 互斥；`checks` 會依 YAML 順序執行，並在 health API 中依序命名為 `check-1`、`check-2` 等；`["NONE"]` 會停用 healthcheck。probe 在 host 執行，沿用 service 的 working directory 與 environment。健康度只影響 `HEALTH`，不會自動改變 lifecycle 或觸發 restart。

service 啟動依賴使用巢狀 mapping：

~~~yaml
services:
  web:
    depends_on:
      db:
        condition: service_healthy
        restart: true
~~~

`condition` 支援 `service_started`、`service_healthy` 與 `service_completed_successfully`。條件未滿足時 dependent 顯示 `waiting` 且不建立 PID；設定錯誤、未知 dependency 與 cycle 會在 apply 前拒絕。

### schedules

~~~yaml
schedules:
  - name: nightly-job
    cron: "0 2 * * *"
    timezone: Asia/Taipei
    action: run
    command: go
    args: [run, ./examples/one-task, --iterations, "3"]
    concurrency: forbid
    retry:
      retries: 3
      delay: 5s
~~~

| 欄位 | 必填 | 預設值 | 說明 |
| --- | --- | --- | --- |
| name | 是 | 無 | project 內唯一的 schedule 名稱。 |
| cron | 是 | 無 | 五欄位 cron：分、時、日、月、星期。 |
| timezone | 否 | Local | IANA timezone，例如 Asia/Taipei、UTC。 |
| action | 是 | 無 | run、start、stop 或 restart。 |
| target | action 非 run 時必填 | 無 | 要操作的 service name。 |
| command | action=run 時必填 | 無 | 一次性 task executable。 |
| args | 否 | [] | 一次性 task 的參數陣列。 |
| working_dir | 否 | defaults 值 | task 工作目錄。 |
| env | 否 | 繼承環境 | task 環境變數。 |
| concurrency | 否 | forbid | 可選 forbid 或 allow。 |
| retry | 否 | 不重試 | `retries` 表示初次失敗後的重試次數；`delay` 表示每次重試前的等待時間。 |

排程規則：

- daemon 離線期間錯過的排程不補執行。
- forbid 會跳過上一個相同 schedule 尚未完成的執行。
- schedule 執行失敗時，會依 `retry.retries` 與 `retry.delay` 重試；同一次 schedule run 最終只保存一筆 history。
- cron 與 timezone 錯誤會使 config validate／apply 失敗。
- schedule task 日誌會寫入 service log root 下的 schedule-<name> 目錄。

## Service 狀態與重啟

可能的 service lifecycle state：

~~~text
stopped
starting
waiting
running
stopping
exited
backing_off
crash_loop
failed
disabled
unknown
~~~

`waiting` 表示 depends_on 條件尚未滿足；它不代表 service unhealthy，也不會建立 PID。

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
mango project add demo .\mango.example.yaml
mango daemon start
mango project apply demo
mango ls
~~~

執行一次性 task：

~~~powershell
mango start demo/one-task
mango logs demo/one-task --stream all --follow
~~~

測試排程 task：

~~~powershell
mango schedule ls
mango schedule run demo/nightly-job
mango schedule history
~~~

開機自啟：

~~~powershell
mango startup install
mango startup status
~~~

## 開發與驗證

~~~powershell
go test ./...
go test -race ./...
go vet ./...
~~~

跨平台 build：

~~~powershell
$env:GOOS = "windows"; $env:GOARCH = "amd64"; go build -o bin/mango-windows-amd64.exe ./cmd/mango; go build -o bin/mangod-windows-amd64.exe ./cmd/mangod
$env:GOOS = "linux";   $env:GOARCH = "amd64"; go build -o bin/mango-linux-amd64 ./cmd/mango; go build -o bin/mangod-linux-amd64 ./cmd/mangod
$env:GOOS = "darwin";  $env:GOARCH = "arm64"; go build -o bin/mango-darwin-arm64 ./cmd/mango; go build -o bin/mangod-darwin-arm64 ./cmd/mangod
~~~
