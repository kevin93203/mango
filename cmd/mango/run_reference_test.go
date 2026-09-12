package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
)

const cliRunID = "7f31a2c4-d9e0-4b11-9c8a-1234567890ab"

func TestHumanRunOutputUsesShortIDAndNoTruncRestoresCanonicalID(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON, previousNoTrunc := cliOutput, jsonOutput, noTruncOutput
	t.Cleanup(func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
		noTruncOutput = previousNoTrunc
	})
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	caller := func(method string, _ interface{}) (ipc.Response, error) {
		if method != "task.run" {
			t.Fatalf("method = %q, want task.run", method)
		}
		return ipc.Response{OK: true, Data: json.RawMessage(`{"key":"demo/job","run_id":"7f31a2c4-d9e0-4b11-9c8a-1234567890ab"}`)}, nil
	}
	if err := taskRunCommandWithCaller("demo/job", caller); err != nil {
		t.Fatal(err)
	}
	if text := output.String(); !strings.Contains(text, "run_id=7f31a2c4d9e0") || strings.Contains(text, cliRunID) {
		t.Fatalf("short output = %q", text)
	}

	output.Reset()
	noTruncOutput = true
	if err := taskRunCommandWithCaller("demo/job", caller); err != nil {
		t.Fatal(err)
	}
	if text := output.String(); !strings.Contains(text, "run_id="+cliRunID) {
		t.Fatalf("full output = %q", text)
	}

	output.Reset()
	noTruncOutput = false
	jsonOutput = true
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	if err := taskRunCommandWithCaller("demo/job", caller); err != nil {
		t.Fatal(err)
	}
	if text := output.String(); !strings.Contains(text, cliRunID) {
		t.Fatalf("JSON output = %q, want canonical run ID", text)
	}
}

func TestStructuredReferenceErrorPrintsShortOrFullCandidates(t *testing.T) {
	app, output := newTestRoot(t)
	err := &ipc.CallError{
		Code:    "RUN_ID_AMBIGUOUS",
		Message: "run reference is ambiguous",
		Candidates: []ipc.ErrorCandidate{{
			Ref: "7f31a2c4d9e0", RunID: cliRunID,
		}},
	}
	app.printCommandError(err)
	if text := output.String(); !strings.Contains(text, "7f31a2c4d9e0") || strings.Contains(text, cliRunID) {
		t.Fatalf("short candidate error = %q", text)
	}

	output.Reset()
	app.noTrunc = true
	app.printCommandError(err)
	if text := output.String(); !strings.Contains(text, cliRunID) {
		t.Fatalf("full candidate error = %q", text)
	}
}
