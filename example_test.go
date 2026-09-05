package mango

import (
	"io/fs"
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

func TestExampleFilesMatchRepositorySources(t *testing.T) {
	paths := []string{
		"examples/api/main.go",
		"examples/one-task/main.go",
		"examples/tasks/emit.go",
	}
	for _, path := range paths {
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := fs.ReadFile(ExampleFiles(), path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("embedded %s does not match repository source", path)
		}
	}
}
