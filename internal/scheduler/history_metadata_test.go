package scheduler

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSafeTaskMetadataRedactsSensitiveFlagsAndValues(t *testing.T) {
	metadata := SafeTaskMetadata(
		"runner",
		[]string{"--token", "token-value", "--password=pass-value", "--region", "prod", "token-value"},
		"/work",
		map[string]string{
			"API_TOKEN": "token-value",
			"PASSWORD":  "pass-value",
			"REGION":    "prod",
		},
	)

	if metadata.Command != "runner" || metadata.WorkingDir != "/work" {
		t.Fatalf("metadata = %+v, want command and working directory preserved", metadata)
	}
	wantArgs := []string{"--token", redactedValue, "--password=" + redactedValue, "--region", "prod", redactedValue}
	if !reflect.DeepEqual(metadata.Args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", metadata.Args, wantArgs)
	}
	if !reflect.DeepEqual(metadata.EnvKeys, []string{"API_TOKEN", "PASSWORD", "REGION"}) {
		t.Fatalf("env keys = %#v, want sorted keys", metadata.EnvKeys)
	}
	if !metadata.ArgsRedacted {
		t.Fatal("ArgsRedacted = false, want true")
	}
}

func TestSafeTaskMetadataRedactsSensitiveFlagWithoutEnvironment(t *testing.T) {
	metadata := SafeTaskMetadata("runner", []string{"--api-key", "value", "--name", "demo"}, ".", nil)
	wantArgs := []string{"--api-key", redactedValue, "--name", "demo"}
	if !reflect.DeepEqual(metadata.Args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", metadata.Args, wantArgs)
	}
	if !metadata.ArgsRedacted {
		t.Fatal("ArgsRedacted = false, want true")
	}
}

func TestSafeTaskMetadataOmitsEnvironmentValuesAndKeepsNonSensitiveArgs(t *testing.T) {
	metadata := SafeTaskMetadata("runner", []string{"--region", "prod"}, ".", map[string]string{
		"REGION": "prod",
	})
	if !reflect.DeepEqual(metadata.Args, []string{"--region", "prod"}) {
		t.Fatalf("args = %#v, want non-sensitive args unchanged", metadata.Args)
	}
	if metadata.ArgsRedacted {
		t.Fatal("ArgsRedacted = true, want false")
	}
	for _, key := range metadata.EnvKeys {
		if key == "prod" {
			t.Fatal("env keys unexpectedly contain environment value")
		}
	}
}

func TestTaskRecordJSONContainsMetadataWithoutEnvironmentValues(t *testing.T) {
	record := TaskRecord{
		Command:      "runner",
		Args:         []string{"--token", redactedValue},
		WorkingDir:   "/work",
		EnvKeys:      []string{"API_TOKEN"},
		ArgsRedacted: true,
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(data)
	if !strings.Contains(encoded, `"EnvKeys":["API_TOKEN"]`) || !strings.Contains(encoded, `"Args":["--token","\u003credacted\u003e"]`) {
		t.Fatalf("JSON = %s, want safe task metadata", encoded)
	}
	if strings.Contains(encoded, "secret-token") {
		t.Fatalf("JSON = %s, must not contain raw environment value", encoded)
	}
}
