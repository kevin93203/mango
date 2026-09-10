package process

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/resources"
)

type Spec struct {
	Command    string
	Args       []string
	WorkingDir string
	Env        map[string]string
	User       string
	Group      string
	Identity   *resources.Identity
	Stdout     io.Writer
	Stderr     io.Writer
	Resources  *resources.Policy
}

type Result struct {
	ExitCode  int
	Err       error
	StartedAt time.Time
	EndedAt   time.Time
}

type Handle struct {
	cmd       *exec.Cmd
	startedAt time.Time
	done      chan struct{}
	mu        sync.RWMutex
	result    Result
	platform  platformState
}

func Start(spec Spec) (*Handle, error) {
	if spec.Command == "" {
		return nil, fmt.Errorf("command is required")
	}
	cmd := exec.Command(spec.Command, spec.Args...)
	cmd.Dir = spec.WorkingDir
	if spec.Env != nil {
		keys := make([]string, 0, len(spec.Env))
		for key := range spec.Env {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		cmd.Env = make([]string, 0, len(keys))
		for _, key := range keys {
			cmd.Env = append(cmd.Env, key+"="+spec.Env[key])
		}
	}
	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr
	cmd.WaitDelay = time.Second
	if err := prepareCommand(cmd, spec); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	h := &Handle{cmd: cmd, startedAt: startedAt, done: make(chan struct{})}
	if err := attachProcessTree(h, spec); err != nil {
		abortProcessTree(cmd.Process.Pid)
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	go h.wait()
	return h, nil
}

func (h *Handle) wait() {
	err := h.cmd.Wait()
	releaseProcessTree(h)
	exitCode := -1
	if h.cmd.ProcessState != nil {
		exitCode = h.cmd.ProcessState.ExitCode()
	}
	h.mu.Lock()
	h.result = Result{ExitCode: exitCode, Err: err, StartedAt: h.startedAt, EndedAt: time.Now()}
	h.mu.Unlock()
	close(h.done)
}

func (h *Handle) Wait() Result {
	<-h.done
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.result
}

func (h *Handle) Done() <-chan struct{} {
	return h.done
}

func (h *Handle) PID() int {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return 0
	}
	return h.cmd.Process.Pid
}

func (h *Handle) StartedAt() time.Time {
	return h.startedAt
}

func (h *Handle) Stop(timeout time.Duration) error {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return nil
	}
	select {
	case <-h.done:
		return nil
	default:
	}
	if err := gracefulStop(h); err != nil {
		// Some platforms do not support a graceful interrupt. Force-stop in
		// that case instead of waiting for the graceful timeout first.
		_ = forceStop(h)
		<-h.done
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-h.done:
		return nil
	case <-timer.C:
		_ = forceStop(h)
		<-h.done
		return nil
	}
}

// ForceStop immediately terminates the process tree managed by the handle and
// waits until its result has been collected.
func (h *Handle) ForceStop() error {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return nil
	}
	select {
	case <-h.done:
		return nil
	default:
	}
	err := forceStop(h)
	<-h.done
	return err
}

func (h *Handle) CommandLine() string {
	if h == nil || h.cmd == nil {
		return ""
	}
	return h.cmd.String()
}

func (h *Handle) Process() *os.Process {
	if h == nil || h.cmd == nil {
		return nil
	}
	return h.cmd.Process
}
