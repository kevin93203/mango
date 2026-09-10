package shim

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/secrets"
)

func TestNewBootstrapUsesExplicitSecretReferences(t *testing.T) {
	spec := config.EffectiveService{
		Project: "demo", Name: "api", Command: "fixture",
		Environment: map[string]string{"PLAIN": "__MANGO_SECRET_REF__:from_env:LEGAL"},
		EnvironmentRefs: map[string]secrets.Reference{
			"TOKEN": {Provider: "from_env", Name: "TOKEN_VALUE"},
		},
	}
	bootstrap, err := NewBootstrap(spec, "demo/api", "demo/api", "incarnation", "stdout.log", "stderr.log", true)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.SchemaVersion != BootstrapSchemaVersion || bootstrap.ProtocolVersion != ProtocolVersion {
		t.Fatalf("bootstrap versions = schema %d protocol %d", bootstrap.SchemaVersion, bootstrap.ProtocolVersion)
	}
	if bootstrap.SecretRefs["TOKEN"].Provider != "from_env" || bootstrap.SecretRefs["TOKEN"].Name != "TOKEN_VALUE" {
		t.Fatalf("secret reference was not preserved: %+v", bootstrap.SecretRefs)
	}
	if bootstrap.Env["PLAIN"] != "__MANGO_SECRET_REF__:from_env:LEGAL" {
		t.Fatal("plain environment value was rewritten as a secret reference")
	}
	data, marshalErr := json.Marshal(bootstrap)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(data), "TOKEN_VALUE") == false {
		t.Fatal("reference name should be present for diagnostics")
	}
	if strings.Contains(string(data), "secret-value") {
		t.Fatal("secret value was serialized")
	}
}

func TestResolveExecutablePrefersSibling(t *testing.T) {
	dir := t.TempDir()
	cli := filepath.Join(dir, "mangod")
	shimPath := filepath.Join(dir, "mango-shim")
	if runtime.GOOS == "windows" {
		shimPath += ".exe"
	}
	if err := os.WriteFile(cli, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shimPath, nil, 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveExecutableFrom(cli, runtime.GOOS, func(string) (string, error) {
		t.Fatal("PATH lookup should not be used when sibling exists")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != shimPath {
		t.Fatalf("path = %q, want %q", got, shimPath)
	}
}

func TestFingerprintDoesNotDependOnMapIterationOrder(t *testing.T) {
	first := config.EffectiveService{
		Project: "demo", Name: "api", Command: "/bin/api", Supervisor: "shim",
		Args: []string{"--port", "8080"}, WorkingDir: "/tmp", Env: map[string]string{"B": "2", "A": "1"},
		Restart: "always",
	}
	second := first
	second.Env = map[string]string{"A": "1", "B": "2"}
	if fingerprintFor(first) != fingerprintFor(second) {
		t.Fatal("fingerprint changed with environment map insertion order")
	}
}

func TestParseShimTime(t *testing.T) {
	value, err := parseShimTime("1700000000.125Z")
	if err != nil {
		t.Fatal(err)
	}
	if value.Unix() != 1700000000 || value.Nanosecond() != 125000000 {
		t.Fatalf("time = %v", value)
	}
}

func TestFindMatchingRejectsLiveV2ShimState(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "old")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := Bootstrap{SchemaVersion: 1, ProtocolVersion: 2, ServiceKey: "demo/api", Incarnation: "old"}
	if err := writeJSON(filepath.Join(stateDir, "bootstrap.json"), old); err != nil {
		t.Fatal(err)
	}
	status := Status{ShimPID: os.Getpid()}
	if err := writeJSON(filepath.Join(stateDir, "status.json"), status); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := findMatching(context.Background(), root, Bootstrap{ServiceKey: "demo/api"})
	if !errors.Is(err, ErrMigration) {
		t.Fatalf("findMatching error = %v, want ErrMigration", err)
	}
}

func TestFindMatchingCleansDeadV2ShimState(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "old")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := Bootstrap{SchemaVersion: 1, ProtocolVersion: 2, ServiceKey: "demo/api", Incarnation: "old"}
	if err := writeJSON(filepath.Join(stateDir, "bootstrap.json"), old); err != nil {
		t.Fatal(err)
	}
	_, _, found, err := findMatching(context.Background(), root, Bootstrap{ServiceKey: "demo/api"})
	if err != nil || found {
		t.Fatalf("findMatching = found %v, err %v", found, err)
	}
	if _, err := os.Stat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead v2 state still exists: %v", err)
	}
}
