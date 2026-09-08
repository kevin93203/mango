package daemon

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestPhase2IPCResponsesUseProtocolV2(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	response := d.Handle(context.Background(), requestForMethod(t, "health"))
	if response.Version != ipc.ProtocolVersion {
		t.Fatalf("response version = %d, want %d", response.Version, ipc.ProtocolVersion)
	}
}

func TestPhase2ExecutionListRejectsAllAndStatusTogether(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	request := requestForMethod(t, "execution.ls")
	request.Params = json.RawMessage(`{"all":true,"status":"failed"}`)
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "BAD_PARAMS" {
		t.Fatalf("execution.ls response = %+v, want BAD_PARAMS", response)
	}
}

func TestPhase2RetryRejectsMissingTargetBeforeCreatingExecution(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "source-run", Project: "demo", TargetType: "task", Target: "removed",
		Status: scheduler.StatusFailed, Started: timeForDaemonTest(-2), Finished: timeForDaemonTest(-1), ExitCode: 1,
	})
	request := requestForMethod(t, "execution.retry")
	request.Params = json.RawMessage(`{"run_id":"source-run"}`)
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "EXECUTION_RETRY_FAILED" {
		t.Fatalf("execution.retry response = %+v, want EXECUTION_RETRY_FAILED", response)
	}
	rows, err := d.scheduler.ListExecutions(context.Background(), scheduler.ExecutionQuery{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Record.RunID != "source-run" {
		t.Fatalf("executions after rejected retry = %+v, want source only", rows)
	}
}

func timeForDaemonTest(offset int64) time.Time {
	return time.Unix(offset, 0).UTC()
}
