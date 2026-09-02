package process

import (
	"os"
	"testing"
	"time"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	time.Sleep(100 * time.Millisecond)
	os.Exit(0)
}

func TestStartAndWait(t *testing.T) {
	handle, err := Start(Spec{
		Command: os.Args[0],
		Args:    []string{"-test.run=TestHelperProcess", "--"},
		Env:     map[string]string{"GO_WANT_HELPER_PROCESS": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := handle.Wait()
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, err = %v", result.ExitCode, result.Err)
	}
}
