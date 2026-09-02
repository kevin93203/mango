# goserve

goserve 是使用 Go 開發的跨平台 CLI process manager，透過背景 daemon 管理任何可執行檔，不限定 Node.js。

目前提供：

- 多 project、多 process 管理
- 常駐 process 的啟動、停止、重啟與 crash-loop 保護
- stdout／stderr 日誌與大小輪替
- CPU、RSS memory、uptime 監控
- 五欄位 cron 排程與 IANA timezone
- Unix Domain Socket／Windows Named Pipe 本機 IPC
- Windows、Linux、macOS 每使用者開機自啟模板
- goserve monitor 互動式終端監控

## 快速開始

~~~powershell
go run ./cmd/goserve config validate --file .\goserve.toml
go run ./cmd/goserve project add --name demo --file .\goserve.toml
go run ./cmd/goserve daemon run
~~~

另一個終端機執行：

~~~powershell
go run ./cmd/goserve apply --project demo
go run ./cmd/goserve list
go run ./cmd/goserve logs demo/api --follow
go run ./cmd/goserve monitor
~~~

背景啟動與每使用者開機自啟：

~~~powershell
goserve daemon start
goserve startup install
~~~

完整設定範例請參考 goserve.example.toml。

## 測試程式

API server：

~~~powershell
go run ./examples/api --port 8080 --interval 3s
~~~

開啟另一個終端機測試：

~~~powershell
Invoke-WebRequest http://127.0.0.1:8080/
Invoke-WebRequest http://127.0.0.1:8080/error
~~~

一次性 task：

~~~powershell
go run ./examples/one-task --iterations 5 --interval 500ms
go run ./examples/one-task --iterations 3 --fail
~~~

第二個指令會以 exit code 2 結束，可用來測試 goserve 的失敗狀態與重啟策略。

## 設定原則

command 使用直接 executable + args，不經 shell。相對路徑以 TOML 所在目錄為基準。stop 是暫時停止；disable 才會停用 autostart。

## 開發

~~~powershell
go test ./...
go vet ./...
go build -o bin/goserve.exe ./cmd/goserve
~~~
