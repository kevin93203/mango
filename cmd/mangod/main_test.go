package main

import "testing"

func TestMangoDUsage(t *testing.T) {
	for _, args := range [][]string{nil, {}, {"stop"}, {"run", "--json"}, {"run", "--home"}, {"run", "--home", ""}} {
		if err := validateArgs(args); err == nil {
			t.Fatalf("validateArgs(%v) unexpectedly succeeded", args)
		}
	}
	if err := validateArgs([]string{"run"}); err != nil {
		t.Fatalf("validateArgs(run) = %v", err)
	}
	const home = `C:\Users\test user\mango`
	if got, err := parseArgs([]string{"run", "--home", home}); err != nil || got != home {
		t.Fatalf("parseArgs(run --home) = %q, %v", got, err)
	}
}
