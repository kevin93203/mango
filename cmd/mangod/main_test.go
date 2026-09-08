package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestMangoDCommandTreeAndUsage(t *testing.T) {
	root := newRootCommand()
	if command, _, err := root.Find([]string{"run"}); err != nil || command == root || command.Name() != "run" {
		t.Fatalf("run command missing: command=%v err=%v", command, err)
	}
	if command, _, err := root.Find([]string{"completion"}); err == nil && command != root {
		t.Fatal("unexpected completion command")
	}

	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	root.SetArgs(nil)
	if err := root.Execute(); err != nil {
		t.Fatalf("mangod without command = %v", err)
	}
	if !strings.Contains(output.String(), "Usage:") || !strings.Contains(output.String(), "run") {
		t.Fatalf("root help = %q", output.String())
	}
}

func TestMangoDRejectsInvalidArgumentsWithoutStartingDaemon(t *testing.T) {
	cases := [][]string{{"stop"}, {"run", "--json"}, {"run", "--home"}, {"run", "--home", ""}, {"run", "unexpected"}}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			root := newRootCommand()
			var output bytes.Buffer
			root.SetOut(&output)
			root.SetErr(&output)
			root.SetArgs(args)
			if err := root.Execute(); err == nil {
				t.Fatalf("mangod %v unexpectedly succeeded", args)
			}
		})
	}
}

func TestMangoDAcceptsRunHomeFlag(t *testing.T) {
	root := newRootCommand()
	run, _, err := root.Find([]string{"run"})
	if err != nil {
		t.Fatal(err)
	}
	const home = `C:\Users\test user\mango`
	if err := run.ParseFlags([]string{"--home", home}); err != nil {
		t.Fatalf("parse mangod run --home = %v", err)
	}
	if err := run.Args(run, nil); err != nil {
		t.Fatalf("mangod run args = %v", err)
	}
}
