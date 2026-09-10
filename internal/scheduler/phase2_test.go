package scheduler

import (
	"context"
	"testing"
	"time"
)

func TestExecutionListDefaultsToActiveAndSeparatesTerminalRows(t *testing.T) {
	store := newMemoryHistoryRepository()
	active, _, err := store.(interface {
		BeginExecution(context.Context, Record, string, uint64) (Execution, bool, error)
	}).BeginExecution(context.Background(), Record{RunID: "active", Status: StatusRunning}, "", 1)
	if err != nil || active.Record.RunID != "active" {
		t.Fatalf("active execution = %+v, err %v", active, err)
	}
	terminal := Record{RunID: "done", Status: StatusSuccess, TargetType: "task", Target: "job"}
	if err := store.(interface {
		Record(context.Context, Record, int) error
	}).Record(context.Background(), terminal, 0); err != nil {
		t.Fatal(err)
	}
	lister := store
	activeRows, err := lister.ListExecutions(context.Background(), ExecutionQuery{})
	if err != nil || len(activeRows) != 1 || activeRows[0].Record.RunID != "active" {
		t.Fatalf("default execution list = %+v, err %v", activeRows, err)
	}
	allRows, err := lister.ListExecutions(context.Background(), ExecutionQuery{All: true})
	if err != nil || len(allRows) != 2 {
		t.Fatalf("all execution list = %+v, err %v", allRows, err)
	}
	terminalRows, err := lister.ListExecutions(context.Background(), ExecutionQuery{Status: StatusSuccess})
	if err != nil || len(terminalRows) != 1 || terminalRows[0].Record.RunID != "done" {
		t.Fatalf("terminal execution list = %+v, err %v", terminalRows, err)
	}
}

func TestHistoryQueryIsTerminalOnlyAndNewestFirst(t *testing.T) {
	store := newMemoryHistoryRepository()
	writer := store.(interface {
		Record(context.Context, Record, int) error
	})
	for _, record := range []Record{
		{RunID: "old", Status: StatusSuccess, Started: timeForTest(1)},
		{RunID: "new", Status: StatusFailed, Started: timeForTest(2)},
		{RunID: "running", Status: StatusRunning, Started: timeForTest(3)},
	} {
		if err := writer.Record(context.Background(), record, 0); err != nil {
			t.Fatal(err)
		}
	}
	reader := store.(HistoryRepository)
	rows, err := reader.Query(context.Background(), HistoryQuery{Limit: 10})
	if err != nil || len(rows) != 2 || rows[0].RunID != "new" || rows[1].RunID != "old" {
		t.Fatalf("history rows = %+v, err %v", rows, err)
	}
}

func timeForTest(seconds int64) time.Time {
	return time.Unix(seconds, 0).UTC()
}
