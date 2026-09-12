package daemon

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/scheduler"
)

const (
	daemonRunIDA = "7f31a2c4-d9e0-4b11-9c8a-1234567890ab"
	daemonRunIDB = "7f31a2c4-d9e1-4b11-9c8a-1234567890ab"
)

func TestDaemonResolvesExecutionReferencesAndKeepsJSONCanonical(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	for _, test := range []struct {
		id     string
		status string
	}{
		{id: daemonRunIDA, status: scheduler.StatusSuccess},
		{id: daemonRunIDB, status: scheduler.StatusFailed},
	} {
		d.scheduler.RecordExecution(scheduler.Record{
			RunID: test.id, Project: "demo", TargetType: "task", Target: "job",
			Status: test.status, Started: time.Now().UTC(), Finished: time.Now().UTC(),
		})
	}

	request := requestForMethod(t, "execution.get")
	request.Params = json.RawMessage(`{"run_id":"7f31a2c4d9e0"}`)
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("execution.get with prefix failed: %+v", response.Error)
	}
	var info struct {
		RunID string `json:"run_id"`
	}
	if err := decodeTestData(response.Data, &info); err != nil {
		t.Fatal(err)
	}
	if info.RunID != daemonRunIDA {
		t.Fatalf("resolved JSON run_id = %q, want canonical %q", info.RunID, daemonRunIDA)
	}

	request.Params = json.RawMessage(`{"run_id":"7f31a2c4"}`)
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != scheduler.RunReferenceAmbiguousCode {
		t.Fatalf("ambiguous execution.get response = %+v, want %s", response, scheduler.RunReferenceAmbiguousCode)
	}
	if len(response.Error.Candidates) != 2 {
		t.Fatalf("ambiguous candidates = %+v, want two", response.Error.Candidates)
	}
	for _, candidate := range response.Error.Candidates {
		if candidate.Ref == "" || candidate.RunID == "" {
			t.Fatalf("incomplete candidate = %+v", candidate)
		}
		request.Params, _ = json.Marshal(map[string]string{"run_id": candidate.Ref})
		candidateResponse := d.Handle(context.Background(), request)
		if !candidateResponse.OK {
			t.Fatalf("candidate %q failed: %+v", candidate.Ref, candidateResponse.Error)
		}
	}

	request.Params = json.RawMessage(`{"run_id":"7f31a2c"}`)
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != scheduler.RunReferenceTooShortCode {
		t.Fatalf("short execution.get response = %+v, want %s", response, scheduler.RunReferenceTooShortCode)
	}
}

func TestDaemonReferenceResolutionAppliesToHistoryAndControlFlow(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: daemonRunIDA, Project: "demo", TargetType: "task", Target: "job",
		Status: scheduler.StatusSuccess, Started: time.Now().UTC().Add(-time.Second), Finished: time.Now().UTC(),
	})

	for _, method := range []string{"history.get", "execution.get", "execution.logs"} {
		t.Run(method, func(t *testing.T) {
			request := requestForMethod(t, method)
			request.Params = json.RawMessage(`{"run_id":"7f31a2c4d9e0"}`)
			response := d.Handle(context.Background(), request)
			if !response.OK {
				t.Fatalf("%s with prefix failed: %+v", method, response.Error)
			}
		})
	}

}
