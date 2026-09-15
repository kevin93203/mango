package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/observability"
)

func TestMemoryHistoryDeleteProject(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHistoryRepository()
	started := time.Unix(1, 0).UTC()
	for _, record := range []Record{
		{RunID: "demo-run", Project: "demo", TargetType: "task", Target: "build", Status: StatusSuccess, Started: started, Finished: started.Add(time.Second), ExitCode: 0},
		{RunID: "other-run", Project: "other", TargetType: "task", Target: "build", Status: StatusSuccess, Started: started, Finished: started.Add(time.Second), ExitCode: 0},
	} {
		if err := store.Record(ctx, record, 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, event := range []observability.Event{
		{Project: "demo", RunID: "demo-run", Type: "execution.success"},
		{Project: "other", RunID: "other-run", Type: "execution.success"},
	} {
		if _, err := store.AppendEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.AppendAudit(ctx, observability.AuditEntry{Action: "project.remove", Result: "success"}); err != nil {
		t.Fatal(err)
	}
	for _, occurrence := range []ScheduleOccurrence{
		{ID: "demo-occurrence", Project: "demo", Schedule: "nightly", ScheduledAt: started},
		{ID: "other-occurrence", Project: "other", Schedule: "nightly", ScheduledAt: started},
	} {
		if _, _, err := store.ClaimScheduleOccurrence(ctx, occurrence); err != nil {
			t.Fatal(err)
		}
	}
	for _, delivery := range []WebhookDelivery{
		{WebhookKey: "demo/hook", IdempotencyKey: "demo-delivery", RunID: "demo-run"},
		{WebhookKey: "other/hook", IdempotencyKey: "other-delivery", RunID: "other-run"},
	} {
		if _, _, err := store.ClaimWebhookDelivery(ctx, delivery); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecordExecutionEvent(ctx, ExecutionEvent{RunID: "demo-run", Type: "state_transition"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordExecutionEvent(ctx, ExecutionEvent{RunID: "other-run", Type: "state_transition"}); err != nil {
		t.Fatal(err)
	}

	if err := store.DeleteProject(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if rows, err := store.Query(ctx, HistoryQuery{Project: "demo"}); err != nil || len(rows) != 0 {
		t.Fatalf("demo history = %+v, err=%v; want empty", rows, err)
	}
	if rows, err := store.Query(ctx, HistoryQuery{Project: "other"}); err != nil || len(rows) != 1 || rows[0].RunID != "other-run" {
		t.Fatalf("other history = %+v, err=%v; want other-run", rows, err)
	}
	if _, err := store.GetExecution(ctx, "demo-run"); err != ErrExecutionNotFound {
		t.Fatalf("demo execution error = %v, want ErrExecutionNotFound", err)
	}
	if events, err := store.ListExecutionEvents(ctx, "demo-run"); err != nil || len(events) != 0 {
		t.Fatalf("demo execution events = %+v, err=%v; want empty", events, err)
	}
	if events, err := store.ListEvents(ctx, observability.EventQuery{Project: "demo"}); err != nil || len(events) != 0 {
		t.Fatalf("demo events = %+v, err=%v; want empty", events, err)
	}
	if events, err := store.ListEvents(ctx, observability.EventQuery{Project: "other"}); err != nil || len(events) != 1 {
		t.Fatalf("other events = %+v, err=%v; want one", events, err)
	}
	if _, err := store.LatestScheduleOccurrence(ctx, "demo", "nightly"); err != ErrScheduleOccurrenceNotFound {
		t.Fatalf("demo occurrence error = %v, want ErrScheduleOccurrenceNotFound", err)
	}
	if _, err := store.LatestScheduleOccurrence(ctx, "other", "nightly"); err != nil {
		t.Fatalf("other occurrence error = %v", err)
	}
	if _, created, err := store.ClaimWebhookDelivery(ctx, WebhookDelivery{WebhookKey: "demo/hook", IdempotencyKey: "demo-delivery", RunID: "demo-run"}); err != nil || !created {
		t.Fatalf("demo webhook recreated = created %v, err %v; want new delivery", created, err)
	}
	if _, created, err := store.ClaimWebhookDelivery(ctx, WebhookDelivery{WebhookKey: "other/hook", IdempotencyKey: "other-delivery", RunID: "other-run"}); err != nil || created {
		t.Fatalf("other webhook recreated = created %v, err %v; want existing delivery", created, err)
	}
	if audits, err := store.ListAudit(ctx, 0); err != nil || len(audits) != 1 {
		t.Fatalf("audit entries = %+v, err=%v; want one retained entry", audits, err)
	}
	for key := range mustCounters(t, store) {
		if strings.Contains(key, "|demo|") {
			t.Fatalf("demo counter %q remains", key)
		}
	}
}

func mustCounters(t *testing.T, store HistoryRepository) map[string]uint64 {
	t.Helper()
	counters, err := store.Counters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return counters
}
