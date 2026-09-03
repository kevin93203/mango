package main

import (
	"errors"
	"testing"
	"time"
)

func TestWaitForUnavailableWaitsForEndpointToClose(t *testing.T) {
	checks := 0
	err := waitForUnavailable(func() error {
		checks++
		if checks < 3 {
			return nil
		}
		return errors.New("endpoint closed")
	}, 200*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if checks != 3 {
		t.Fatalf("checks = %d, want 3", checks)
	}
}

func TestWaitForUnavailableTimesOut(t *testing.T) {
	err := waitForUnavailable(func() error { return nil }, 5*time.Millisecond, time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout")
	}
}
