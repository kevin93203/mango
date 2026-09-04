package process

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	switch os.Getenv("GO_STOP_HELPER_PROCESS") {
	case "parent":
		time.Sleep(100 * time.Millisecond)
		child := exec.Command(os.Args[0], "-test.run=TestHelperProcess", "--")
		child.Env = []string{
			"GO_WANT_HELPER_PROCESS=1",
			"GO_STOP_HELPER_PROCESS=child",
		}
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(3)
		}
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "child":
		time.Sleep(30 * time.Second)
		os.Exit(0)
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

func TestStopDoesNotWaitForUnsupportedGracefulSignal(t *testing.T) {
	handle, err := Start(Spec{
		Command: os.Args[0],
		Args:    []string{"-test.run=TestHelperProcess", "--"},
		Env: map[string]string{
			"GO_WANT_HELPER_PROCESS": "1",
			"GO_STOP_HELPER_PROCESS": "parent",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	started := time.Now()
	if err := handle.Stop(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 5*time.Second {
		t.Fatalf("stop took %s; expected force stop to happen before the graceful timeout", elapsed)
	}
}

func TestForceStopTerminatesProcessTreeImmediately(t *testing.T) {
	handle, err := Start(Spec{
		Command: os.Args[0],
		Args:    []string{"-test.run=TestHelperProcess", "--"},
		Env: map[string]string{
			"GO_WANT_HELPER_PROCESS": "1",
			"GO_STOP_HELPER_PROCESS": "parent",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	started := time.Now()
	if err := handle.ForceStop(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 5*time.Second {
		t.Fatalf("force stop took %s; expected immediate process-tree termination", elapsed)
	}
}
