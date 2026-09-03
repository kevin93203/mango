package main

import "testing"

func TestMangoDUsage(t *testing.T) {
	for _, args := range [][]string{nil, {}, {"stop"}, {"run", "--json"}} {
		if err := validateArgs(args); err == nil {
			t.Fatalf("validateArgs(%v) unexpectedly succeeded", args)
		}
	}
	if err := validateArgs([]string{"run"}); err != nil {
		t.Fatalf("validateArgs(run) = %v", err)
	}
}
