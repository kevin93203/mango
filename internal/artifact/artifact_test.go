package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestCollectRecordsMetadataAndMissingOutputs(t *testing.T) {
	base := t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "dist"), 0o700); err != nil {
		t.Fatal(err)
	}
	data := []byte("release")
	path := filepath.Join(base, "dist", "release.txt")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	artifacts, err := Collect(base, []string{"dist/release.txt", "dist/missing.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 2 {
		t.Fatalf("artifacts = %#v, want two entries", artifacts)
	}
	hash := sha256.Sum256(data)
	if artifacts[0].Path != "dist/release.txt" || !artifacts[0].Exists || artifacts[0].Size != int64(len(data)) || artifacts[0].SHA256 != hex.EncodeToString(hash[:]) {
		t.Fatalf("existing artifact = %+v, want metadata", artifacts[0])
	}
	if artifacts[1].Path != "dist/missing.txt" || artifacts[1].Exists || artifacts[1].Size != 0 || artifacts[1].SHA256 != "" {
		t.Fatalf("missing artifact = %+v, want absent metadata", artifacts[1])
	}
}

func TestCollectRejectsResolvedPathOutsideBase(t *testing.T) {
	base := t.TempDir()
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(outsidePath, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link.txt")
	if err := os.Symlink(outsidePath, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	artifacts, err := Collect(base, []string{"link.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 || artifacts[0].Exists {
		t.Fatalf("artifacts = %#v, want symlink escape treated as absent", artifacts)
	}
}
