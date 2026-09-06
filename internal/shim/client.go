package shim

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/instance"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
)

const ProtocolVersion = 1

var (
	ErrUnavailable = errors.New("mango-shim is unavailable")
	ErrOrphaned    = errors.New("mango-shim has an orphaned service process")
)

type Bootstrap struct {
	SchemaVersion     int               `json:"schema_version"`
	ProtocolVersion   int               `json:"protocol_version"`
	ServiceKey        string            `json:"service_key"`
	InstanceID        string            `json:"instance_id"`
	Incarnation       string            `json:"incarnation"`
	ConfigFingerprint string            `json:"config_fingerprint"`
	Command           string            `json:"command"`
	Args              []string          `json:"args,omitempty"`
	WorkingDir        string            `json:"working_dir,omitempty"`
	Env               map[string]string `json:"env,omitempty"`
	Autostart         bool              `json:"autostart"`
	Restart           string            `json:"restart"`
	StopTimeoutMS     int64             `json:"stop_timeout_ms"`
	MaxRestarts       int               `json:"max_restarts"`
	RestartWindowMS   int64             `json:"restart_window_ms"`
	StableAfterMS     int64             `json:"stable_after_ms"`
	StdoutPath        string            `json:"stdout_path"`
	StderrPath        string            `json:"stderr_path"`
	LogMaxSize        int64             `json:"log_max_size"`
	LogMaxFiles       int               `json:"log_max_files"`
}

type ExitRecord struct {
	Sequence           uint64 `json:"sequence"`
	ExitCode           *int   `json:"exit_code,omitempty"`
	Signal             *int   `json:"signal,omitempty"`
	StartedAt          string `json:"started_at,omitempty"`
	EndedAt            string `json:"ended_at,omitempty"`
	Intentional        bool   `json:"intentional"`
	DescendantsCleaned bool   `json:"descendants_cleaned"`
	Error              string `json:"error,omitempty"`
}

type Status struct {
	SchemaVersion     int         `json:"schema_version"`
	ProtocolVersion   int         `json:"protocol_version"`
	ServiceKey        string      `json:"service_key"`
	InstanceID        string      `json:"instance_id"`
	Incarnation       string      `json:"incarnation"`
	ConfigFingerprint string      `json:"config_fingerprint"`
	State             string      `json:"state"`
	DesiredState      string      `json:"desired_state"`
	ShimPID           int         `json:"shim_pid"`
	ServicePID        int         `json:"service_pid"`
	ProcessStartToken string      `json:"process_start_token,omitempty"`
	StartedAt         time.Time   `json:"-"`
	StartedAtRaw      string      `json:"started_at,omitempty"`
	UpdatedAt         string      `json:"updated_at"`
	RestartCount      int         `json:"restart_count"`
	NextRestartAt     string      `json:"next_restart_at,omitempty"`
	LastExit          *ExitRecord `json:"last_exit,omitempty"`
	LastError         string      `json:"last_error,omitempty"`
	StdoutPath        string      `json:"stdout_path"`
	StderrPath        string      `json:"stderr_path"`
	JobObject         string      `json:"job_object,omitempty"`
}

type Client struct {
	Endpoint          string
	StateDir          string
	ServiceKey        string
	InstanceID        string
	Incarnation       string
	ConfigFingerprint string
}

func NewBootstrap(spec config.EffectiveService, serviceKey, instanceID, incarnation, stdoutPath, stderrPath string, autostart bool) Bootstrap {
	env := serviceEnvironment(spec)
	return Bootstrap{
		SchemaVersion: 1, ProtocolVersion: ProtocolVersion, ServiceKey: serviceKey,
		InstanceID: instanceID, Incarnation: incarnation, ConfigFingerprint: fingerprintFor(spec),
		Command: spec.Command, Args: append([]string(nil), spec.Args...), WorkingDir: spec.WorkingDir,
		Env: env, Autostart: autostart, Restart: spec.Restart,
		StopTimeoutMS: spec.StopTimeout.Milliseconds(), MaxRestarts: spec.MaxRestarts,
		RestartWindowMS: spec.RestartWindow.Milliseconds(), StableAfterMS: spec.StableAfter.Milliseconds(),
		StdoutPath: stdoutPath, StderrPath: stderrPath, LogMaxSize: spec.LogMaxSize, LogMaxFiles: spec.LogMaxFiles,
	}
}

func (c *Client) Hello(ctx context.Context) error {
	var result struct {
		ProtocolVersion   int      `json:"protocol_version"`
		InstanceID        string   `json:"instance_id"`
		ServiceKey        string   `json:"service_key"`
		ConfigFingerprint string   `json:"config_fingerprint"`
		Capabilities      []string `json:"capabilities"`
	}
	if err := c.call(ctx, "hello", map[string]string{
		"instance_id": c.InstanceID, "service_key": c.ServiceKey,
		"config_fingerprint": c.ConfigFingerprint,
	}, &result); err != nil {
		return err
	}
	if result.ProtocolVersion != ProtocolVersion || result.InstanceID != c.InstanceID || result.ServiceKey != c.ServiceKey {
		return fmt.Errorf("shim handshake mismatch")
	}
	return nil
}

func (c *Client) Status(ctx context.Context) (Status, error) {
	var result Status
	if err := c.call(ctx, "status", nil, &result); err != nil {
		return Status{}, err
	}
	if result.StartedAtRaw != "" {
		if value, err := parseShimTime(result.StartedAtRaw); err == nil {
			result.StartedAt = value
		}
	}
	return result, nil
}

func (c *Client) Start(ctx context.Context) error {
	var result Status
	return c.call(ctx, "start", nil, &result)
}

func (c *Client) Stop(ctx context.Context, timeout time.Duration) error {
	var result Status
	return c.call(ctx, "stop", map[string]int64{"timeout_ms": timeout.Milliseconds()}, &result)
}

func (c *Client) Restart(ctx context.Context) error {
	var result Status
	return c.call(ctx, "restart", nil, &result)
}

func (c *Client) Shutdown(ctx context.Context, timeout time.Duration) error {
	var result Status
	return c.call(ctx, "shutdown", map[string]int64{"timeout_ms": timeout.Milliseconds()}, &result)
}

func (c *Client) call(ctx context.Context, method string, params interface{}, result interface{}) error {
	request, err := ipc.NewRequest(method, params)
	if err != nil {
		return err
	}
	response, err := ipc.CallEndpoint(ctx, c.Endpoint, request)
	if err != nil {
		if errors.Is(err, ipc.ErrDaemonUnavailable) {
			return ErrUnavailable
		}
		if response.Error != nil {
			switch response.Error.Code {
			case "ORPHANED":
				return fmt.Errorf("%w: %s", ErrOrphaned, response.Error.Message)
			case "PERMISSION_DENIED":
				return fmt.Errorf("permission denied: %s", response.Error.Message)
			}
		}
		return err
	}
	if result == nil || response.Data == nil {
		return nil
	}
	data, ok := response.Data.(json.RawMessage)
	if !ok {
		data, err = json.Marshal(response.Data)
		if err != nil {
			return err
		}
	}
	return json.Unmarshal(data, result)
}

func ResolveExecutable(cliExecutable string) (string, error) {
	return ResolveExecutableFrom(cliExecutable, runtime.GOOS, exec.LookPath)
}

func ResolveExecutableFrom(cliExecutable, goos string, lookPath func(string) (string, error)) (string, error) {
	name := "mango-shim"
	if goos == "windows" {
		name += ".exe"
	}
	if cliExecutable != "" {
		candidate := filepath.Join(filepath.Dir(cliExecutable), name)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() &&
			(goos == "windows" || info.Mode()&0o111 != 0) {
			return candidate, nil
		}
	}
	if lookPath != nil {
		if path, err := lookPath(name); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("mango-shim executable not found; install mango, mangod, and mango-shim together or add %s to PATH", name)
}

func StartOrAttach(ctx context.Context, layout paths.Layout, executable string, bootstrap Bootstrap) (*Client, Status, error) {
	root := filepath.Join(layout.Runtime, "shims", hashKey(bootstrap.ServiceKey))
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, Status{}, err
	}
	for {
		client, status, found, err := findMatching(ctx, root, bootstrap)
		if err != nil {
			return nil, Status{}, err
		}
		if found {
			return client, status, nil
		}
		createLock, err := acquireCreationLock(ctx, filepath.Join(root, ".create.lock"))
		if err != nil {
			return nil, Status{}, err
		}
		// Another starter may have published a matching shim while this
		// caller was waiting for the creation lock.
		client, status, found, err = findMatching(ctx, root, bootstrap)
		if err != nil {
			_ = createLock.Close()
			return nil, Status{}, err
		}
		if found {
			_ = createLock.Close()
			return client, status, nil
		}
		if bootstrap.Incarnation == "" {
			bootstrap.Incarnation = newID()
		}
		stateDir := filepath.Join(root, bootstrap.Incarnation)
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			_ = createLock.Close()
			return nil, Status{}, err
		}
		endpoint := endpointForState(stateDir)
		if err := prepareEndpoint(endpoint); err != nil {
			_ = createLock.Close()
			return nil, Status{}, err
		}
		if err := writeJSON(filepath.Join(stateDir, "bootstrap.json"), bootstrap); err != nil {
			_ = createLock.Close()
			return nil, Status{}, err
		}
		if err := writeText(filepath.Join(stateDir, "endpoint"), endpoint); err != nil {
			_ = createLock.Close()
			return nil, Status{}, err
		}
		cmd := exec.Command(executable, "run", "--state-dir", stateDir, "--endpoint", endpoint)
		configureShimCommand(cmd)
		if err := detachShimOutput(cmd, layout.DaemonLog); err != nil {
			_ = createLock.Close()
			return nil, Status{}, err
		}
		if err := cmd.Start(); err != nil {
			closeShimOutput(cmd)
			_ = createLock.Close()
			return nil, Status{}, err
		}
		closeShimOutput(cmd)
		go func() { _ = cmd.Wait() }()
		client = &Client{
			Endpoint: endpoint, StateDir: stateDir, ServiceKey: bootstrap.ServiceKey,
			InstanceID: bootstrap.InstanceID, Incarnation: bootstrap.Incarnation,
			ConfigFingerprint: bootstrap.ConfigFingerprint,
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			helloCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			err = client.Hello(helloCtx)
			cancel()
			if err == nil {
				status, err = client.Status(ctx)
				if err == nil {
					_ = createLock.Close()
					return client, status, nil
				}
			}
			select {
			case <-ctx.Done():
				_ = createLock.Close()
				return nil, Status{}, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		_ = createLock.Close()
		return nil, Status{}, fmt.Errorf("mango-shim did not become ready for %s", bootstrap.ServiceKey)
	}
}

func acquireCreationLock(ctx context.Context, path string) (*instance.Lock, error) {
	for {
		lock, err := instance.Acquire(path)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, instance.ErrAlreadyRunning) {
			return nil, err
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func closeShimOutput(cmd *exec.Cmd) {
	seen := map[*os.File]bool{}
	for _, writer := range []interface{}{cmd.Stdout, cmd.Stderr} {
		file, ok := writer.(*os.File)
		if !ok || file == nil || seen[file] {
			continue
		}
		seen[file] = true
		_ = file.Close()
	}
}

// AttachExisting reconnects to a matching live shim without creating a new
// supervisor. It is used during daemon startup so non-autostart services that
// were already running are still visible after mangod is restarted.
func AttachExisting(ctx context.Context, layout paths.Layout, desired Bootstrap) (*Client, Status, bool, error) {
	root := filepath.Join(layout.Runtime, "shims", hashKey(desired.ServiceKey))
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return nil, Status{}, false, nil
	} else if err != nil {
		return nil, Status{}, false, err
	}
	return findMatching(ctx, root, desired)
}

func findMatching(ctx context.Context, root string, desired Bootstrap) (*Client, Status, bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, Status{}, false, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		stateDir := filepath.Join(root, entry.Name())
		var existing Bootstrap
		if err := readJSON(filepath.Join(stateDir, "bootstrap.json"), &existing); err != nil {
			continue
		}
		if existing.ServiceKey != desired.ServiceKey {
			continue
		}
		endpoint := endpointForState(stateDir)
		if data, readErr := os.ReadFile(filepath.Join(stateDir, "endpoint")); readErr == nil && strings.TrimSpace(string(data)) != "" {
			endpoint = strings.TrimSpace(string(data))
		}
		client := &Client{
			Endpoint: endpoint, StateDir: stateDir, ServiceKey: existing.ServiceKey,
			InstanceID: existing.InstanceID, Incarnation: existing.Incarnation,
			ConfigFingerprint: existing.ConfigFingerprint,
		}
		status, statusErr := readStatus(stateDir)
		if existing.ConfigFingerprint == desired.ConfigFingerprint {
			helloCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			helloErr := client.Hello(helloCtx)
			cancel()
			if helloErr != nil && statusErr == nil && status.ShimPID > 0 && processIsAlive(status.ShimPID) {
				helloErr = retryHello(ctx, client, helloErr, 2*time.Second)
			}
			if helloErr == nil {
				freshStatus, freshErr := client.Status(ctx)
				if freshErr == nil {
					return client, freshStatus, true, nil
				}
				return nil, Status{}, false, fmt.Errorf("%w: %s", ErrUnavailable, desired.ServiceKey)
			}
			if statusErr == nil && status.ShimPID > 0 && processIsAlive(status.ShimPID) {
				return nil, Status{}, false, fmt.Errorf("%w: %s", ErrUnavailable, desired.ServiceKey)
			}
			if statusErr == nil && status.ServicePID > 0 && processIsAliveWithToken(status.ServicePID, status.ProcessStartToken) {
				return nil, Status{}, false, fmt.Errorf("%w: %s", ErrOrphaned, desired.ServiceKey)
			}
			if pid, pidErr := readPID(filepath.Join(stateDir, "shim.pid")); pidErr == nil && processIsAlive(pid) {
				return nil, Status{}, false, fmt.Errorf("%w: %s", ErrUnavailable, desired.ServiceKey)
			}
			if statusErr == nil {
				cleanupDeadState(stateDir, endpoint)
			}
			continue
		}
		stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		stopErr := client.Shutdown(stopCtx, 5*time.Second)
		cancel()
		if stopErr == nil {
			continue
		}
		if statusErr == nil && status.ShimPID > 0 && processIsAlive(status.ShimPID) {
			return nil, Status{}, false, fmt.Errorf("old shim for %s is still running: %w", desired.ServiceKey, stopErr)
		}
		if statusErr == nil && status.ServicePID > 0 && processIsAliveWithToken(status.ServicePID, status.ProcessStartToken) {
			return nil, Status{}, false, fmt.Errorf("%w: %s", ErrOrphaned, desired.ServiceKey)
		}
		if pid, pidErr := readPID(filepath.Join(stateDir, "shim.pid")); pidErr == nil && processIsAlive(pid) {
			return nil, Status{}, false, fmt.Errorf("old shim for %s is still running: %w", desired.ServiceKey, stopErr)
		}
		if statusErr == nil {
			cleanupDeadState(stateDir, endpoint)
		}
	}
	return nil, Status{}, false, nil
}

func cleanupDeadState(stateDir, endpoint string) {
	if runtime.GOOS != "windows" {
		_ = os.Remove(endpoint)
	}
	_ = os.RemoveAll(stateDir)
}

func retryHello(ctx context.Context, client *Client, lastErr error, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		helloCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		err := client.Hello(helloCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func readPID(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid PID file %s", path)
	}
	return pid, nil
}

func readStatus(stateDir string) (Status, error) {
	var status Status
	err := readJSON(filepath.Join(stateDir, "status.json"), &status)
	return status, err
}

func fingerprintFor(spec config.EffectiveService) string {
	payload := struct {
		Project       string
		Name          string
		Command       string
		Supervisor    string
		Args          []string
		WorkingDir    string
		Env           map[string]string
		Restart       string
		StopTimeout   int64
		MaxRestarts   int
		RestartWindow int64
		StableAfter   int64
		LogMaxSize    int64
		LogMaxFiles   int
	}{
		Project: spec.Project, Name: spec.Name, Command: spec.Command, Supervisor: spec.Supervisor, Args: spec.Args,
		WorkingDir: spec.WorkingDir, Env: serviceEnvironment(spec), Restart: spec.Restart,
		StopTimeout: spec.StopTimeout.Milliseconds(), MaxRestarts: spec.MaxRestarts,
		RestartWindow: spec.RestartWindow.Milliseconds(), StableAfter: spec.StableAfter.Milliseconds(),
		LogMaxSize: spec.LogMaxSize, LogMaxFiles: spec.LogMaxFiles,
	}
	data, _ := json.Marshal(payload)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func serviceEnvironment(spec config.EffectiveService) map[string]string {
	if spec.Env != nil {
		result := make(map[string]string, len(spec.Env))
		for key, value := range spec.Env {
			result[key] = value
		}
		return result
	}
	result := make(map[string]string, len(spec.Environment))
	for key, value := range spec.Environment {
		result[key] = value
	}
	return result
}

func hashKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func newID() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err == nil {
		return hex.EncodeToString(data[:])
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func writeJSON(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(data, '\n'))
}

func readJSON(path string, target interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func writeText(path, value string) error {
	return writeFileAtomic(path, []byte(value+"\n"))
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".mango-shim-*.tmp")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func parseShimTime(value string) (time.Time, error) {
	value = strings.TrimSuffix(value, "Z")
	parts := strings.SplitN(value, ".", 2)
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, errors.New("invalid shim timestamp")
	}
	nanos := int64(0)
	if len(parts) == 2 {
		fraction := parts[1]
		if len(fraction) > 9 {
			fraction = fraction[:9]
		}
		for len(fraction) < 9 {
			fraction += "0"
		}
		nanos, err = strconv.ParseInt(fraction, 10, 32)
		if err != nil {
			return time.Time{}, errors.New("invalid shim timestamp")
		}
	}
	return time.Unix(seconds, nanos), nil
}
