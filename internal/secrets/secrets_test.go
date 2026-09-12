package secrets

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolverSupportsEnvironmentAndFileReferences(t *testing.T) {
	t.Setenv("MANGO_TEST_SECRET", "env-value")
	path := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(path, []byte("file-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env, redactor, err := ResolveMap(context.Background(), map[string]string{"PLAIN": "ok"}, map[string]Reference{
		"ENV":  {Provider: "from_env", Name: "MANGO_TEST_SECRET"},
		"FILE": {Provider: "from_file", Name: path},
	})
	if err != nil {
		t.Fatal(err)
	}
	if env["ENV"] != "env-value" || env["FILE"] != "file-value" || env["PLAIN"] != "ok" {
		t.Fatalf("resolved environment = %#v", env)
	}
	if got := redactor.Redact("env-value file-value"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redacted value = %q", got)
	}
}

func TestReferenceValidationRejectsMalformedProvider(t *testing.T) {
	if _, err := Parse("env:"); err == nil {
		t.Fatal("empty environment reference was accepted")
	}
	if _, err := Parse("vault:token"); err == nil {
		t.Fatal("unsupported provider was accepted")
	}
	if got := (&Redactor{}).Redact(strings.TrimSpace("")); got != "" {
		t.Fatalf("empty redaction = %q", got)
	}
}
