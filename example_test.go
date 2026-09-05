package mango

import (
	"os"
	"testing"
)

func TestExampleConfigMatchesRepositoryFile(t *testing.T) {
	want, err := os.ReadFile("mango.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := ExampleConfig(); got != string(want) {
		t.Fatal("embedded example does not match mango.example.yaml")
	}
}
