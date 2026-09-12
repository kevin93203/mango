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

func TestExecutionListTaskTargetIncludesWorkflowRoots(t *testing.T) {
	store := newMemoryHistoryRepository()
	writer := store.(interface {
		Record(context.Context, Record, int) error
	})
	for _, record := range []Record{
		{RunID: "direct", Status: StatusSuccess, TargetType: "task", Target: "compile"},
		{RunID: "workflow", Status: StatusSuccess, TargetType: "workflow", Target: "pipeline", Tasks: []TaskRecord{{Task: "compile"}, {Task: "lint"}}},
		{RunID: "other", Status: StatusSuccess, TargetType: "workflow", Target: "other", Tasks: []TaskRecord{{Task: "test"}}},
	} {
		if err := writer.Record(context.Background(), record, 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		name       string
		query      ExecutionQuery
		wantRunIDs map[string]bool
	}{
		{name: "task target", query: ExecutionQuery{All: true, TargetType: "task", Target: "compile"}, wantRunIDs: map[string]bool{"direct": true, "workflow": true}},
		{name: "nested task target", query: ExecutionQuery{All: true, TargetType: "task", Target: "lint"}, wantRunIDs: map[string]bool{"workflow": true}},
		{name: "untyped target", query: ExecutionQuery{All: true, Target: "lint"}, wantRunIDs: map[string]bool{"workflow": true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, err := store.ListExecutions(context.Background(), test.query)
			if err != nil {
				t.Fatal(err)
			}
			got := make(map[string]bool, len(rows))
			for _, row := range rows {
				got[row.Record.RunID] = true
			}
			if len(got) != len(test.wantRunIDs) {
				t.Fatalf("run ids = %v, want %v", got, test.wantRunIDs)
			}
			for runID := range test.wantRunIDs {
				if !got[runID] {
					t.Fatalf("run ids = %v, want %v", got, test.wantRunIDs)
				}
			}
		})
	}
}

func timeForTest(seconds int64) time.Time {
	return time.Unix(seconds, 0).UTC()
}
