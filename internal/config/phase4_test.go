package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSecretEnvironmentAndResourcePolicyDecode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mango.yaml")
	data := []byte(`version: 4
services:
  worker:
    command: worker
    environment:
      PLAIN: production
      TOKEN:
        from_env: MANGO_TOKEN
      CERT:
        from_file: ./cert-token
    run_as:
      user: mango
    resources:
      process_limit: 100
      memory: 512MiB
      cpu_percent: 80
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	service := file.Services["worker"]
	if service.Environment["PLAIN"] != "production" || service.EnvironmentRefs["TOKEN"].Provider != "from_env" || service.EnvironmentRefs["CERT"].Provider != "from_file" {
		t.Fatalf("decoded service environment = %+v refs=%+v", service.Environment, service.EnvironmentRefs)
	}
	if service.Resources == nil || service.Resources.Memory != "512MiB" || service.Resources.CPUPercent != 80 || service.RunAs.User != "mango" {
		t.Fatalf("decoded policy = %+v run_as=%+v", service.Resources, service.RunAs)
	}
}
