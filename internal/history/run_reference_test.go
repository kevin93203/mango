package history

import (
	"context"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/scheduler"
)

func TestFindExecutionRunIDsByPrefixOnlyReturnsRootIDs(t *testing.T) {
	repository := openTestRepository(t)
	for _, runID := range []string{
		"7f31a2c4-d9e0-4b11-9c8a-1234567890ab",
		"7f31a2c4-d9e1-4b11-9c8a-1234567890ab",
	} {
		if err := repository.Record(context.Background(), scheduler.Record{
			RunID: runID, Project: "demo", TargetType: "workflow", Target: "release",
			Status: scheduler.StatusSuccess, Started: time.Now().UTC(), Finished: time.Now().UTC(),
			Tasks: []scheduler.TaskRecord{{RunID: "7f31a2c4-child", ParentRunID: runID}},
		}, 0); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := repository.FindExecutionRunIDsByPrefix(context.Background(), "7f31a2c4-d9e0")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "7f31a2c4-d9e0-4b11-9c8a-1234567890ab" {
		t.Fatalf("prefix IDs = %v", ids)
	}

	ids, err = repository.FindExecutionRunIDsByPrefix(context.Background(), "7f31a2c4")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("root IDs = %v, want two", ids)
	}
}

func TestFindExecutionRunIDsByPrefixEscapesLikeWildcards(t *testing.T) {
	repository := openTestRepository(t)
	for _, runID := range []string{"legacy_%_run", "legacy-plain", "legacy~tilde"} {
		if err := repository.Record(context.Background(), scheduler.Record{
			RunID: runID, Status: scheduler.StatusSuccess, Started: time.Now().UTC(), Finished: time.Now().UTC(),
		}, 0); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := repository.FindExecutionRunIDsByPrefix(context.Background(), "legacy_%")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "legacy_%_run" {
		t.Fatalf("escaped prefix IDs = %v", ids)
	}
	ids, err = repository.FindExecutionRunIDsByPrefix(context.Background(), "legacy~")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "legacy~tilde" {
		t.Fatalf("escaped escape-character IDs = %v", ids)
	}
}
