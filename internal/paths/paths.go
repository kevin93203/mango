package paths

import (
	"os"
	"path/filepath"
)

type Layout struct {
	Root          string
	Runtime       string
	Logs          string
	State         string
	ScheduleState string
	Registry      string
	DaemonConfig  string
	SocketPath    string
	DaemonLog     string
	PIDFile       string
	LockPath      string
}

func Default() (Layout, error) {
	if root := os.Getenv("MANGO_HOME"); root != "" {
		root, err := filepath.Abs(root)
		if err != nil {
			return Layout{}, err
		}
		return Layout{
			Root: root, Runtime: filepath.Join(root, "runtime"), Logs: filepath.Join(root, "logs"),
			State: filepath.Join(root, "state"), ScheduleState: filepath.Join(root, "state", "schedules.json"), Registry: filepath.Join(root, "projects.json"),
			DaemonConfig: filepath.Join(root, "daemon.yaml"),
			SocketPath:   filepath.Join(root, "runtime", "mango.sock"),
			DaemonLog:    filepath.Join(root, "daemon.log"), PIDFile: filepath.Join(root, "runtime", "daemon.pid"),
			LockPath: filepath.Join(root, "runtime", "daemon.lock"),
		}, nil
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return Layout{}, err
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return Layout{}, err
	}
	root := filepath.Join(config, "mango")
	data := filepath.Join(cache, "mango")
	runtime := filepath.Join(root, "runtime")
	return Layout{
		Root: root, Runtime: runtime, Logs: filepath.Join(data, "logs"),
		State: filepath.Join(data, "state"), ScheduleState: filepath.Join(data, "state", "schedules.json"), Registry: filepath.Join(root, "projects.json"),
		DaemonConfig: filepath.Join(root, "daemon.yaml"),
		SocketPath:   filepath.Join(runtime, "mango.sock"),
		DaemonLog:    filepath.Join(data, "daemon.log"), PIDFile: filepath.Join(runtime, "daemon.pid"),
		LockPath: filepath.Join(runtime, "daemon.lock"),
	}, nil
}

func Ensure(layout Layout) error {
	for _, dir := range []string{layout.Root, layout.Runtime, layout.Logs, layout.State} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}
