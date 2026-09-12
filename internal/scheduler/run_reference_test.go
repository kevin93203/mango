package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

const (
	resolverRunIDA = "7f31a2c4-d9e0-4b11-9c8a-1234567890ab"
	resolverRunIDB = "7f31a2c4-d9e1-4b11-9c8a-1234567890ab"
)

func TestResolveExecutionRunIDSupportsUniquePrefixesAndLegacyExactIDs(t *testing.T) {
	scheduler := New(nil)
	for _, test := range []struct {
		id     string
		status string
	}{
		{id: resolverRunIDA, status: StatusRunning},
		{id: resolverRunIDB, status: StatusSuccess},
		{id: "7f31a2c4", status: StatusFailed},
		{id: "legacy-run-1234", status: StatusCancelled},
	} {
		if _, _, err := scheduler.BeginExecution(context.Background(), Record{
			RunID: test.id, Status: test.status, Started: time.Now().UTC(),
		}, "", 0); err != nil {
			t.Fatalf("BeginExecution(%q): %v", test.id, err)
		}
	}

	for _, test := range []struct {
		name      string
		reference string
		want      string
	}{
		{name: "compact prefix", reference: "7f31a2c4d9e0", want: resolverRunIDA},
		{name: "dashed uppercase prefix", reference: "7F31A2C4-D9E0", want: resolverRunIDA},
		{name: "legacy exact short ID", reference: "7f31a2c4", want: "7f31a2c4"},
		{name: "legacy prefix", reference: "legacy-run", want: "legacy-run-1234"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := scheduler.ResolveExecutionRunID(context.Background(), test.reference)
			if err != nil || got != test.want {
				t.Fatalf("ResolveExecutionRunID(%q) = %q, %v; want %q", test.reference, got, err, test.want)
			}
		})
	}
}

func TestResolveExecutionRunIDRejectsShortAndAmbiguousReferences(t *testing.T) {
	scheduler := New(nil)
	for _, id := range []string{resolverRunIDA, resolverRunIDB} {
		if _, _, err := scheduler.BeginExecution(context.Background(), Record{RunID: id, Status: StatusRunning}, "", 0); err != nil {
			t.Fatal(err)
		}
	}

	_, err := scheduler.ResolveExecutionRunID(context.Background(), "7f31a2c")
	var referenceErr *RunReferenceError
	if !errors.As(err, &referenceErr) || referenceErr.Code != RunReferenceTooShortCode {
		t.Fatalf("short reference error = %v, want %s", err, RunReferenceTooShortCode)
	}

	_, err = scheduler.ResolveExecutionRunID(context.Background(), "7f31a2c4")
	if !errors.As(err, &referenceErr) || referenceErr.Code != RunReferenceAmbiguousCode {
		t.Fatalf("ambiguous reference error = %v, want %s", err, RunReferenceAmbiguousCode)
	}
	if len(referenceErr.Candidates) != 2 {
		t.Fatalf("ambiguous candidates = %+v, want two", referenceErr.Candidates)
	}
	for _, candidate := range referenceErr.Candidates {
		resolved, candidateErr := scheduler.ResolveExecutionRunID(context.Background(), candidate.Ref)
		if candidateErr != nil || resolved != candidate.RunID {
			t.Errorf("candidate %+v resolved to %q, %v", candidate, resolved, candidateErr)
		}
	}

	if _, err := scheduler.ResolveExecutionRunID(context.Background(), "ffffffff"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("missing reference error = %v, want ErrExecutionNotFound", err)
	}
}
