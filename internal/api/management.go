package api

// ProjectMutationResult is the stable result for project lifecycle changes.
// Runtime convergence remains observable through ProjectStatus.
type ProjectMutationResult struct {
	Project    string `json:"project"`
	NewProject string `json:"new_project,omitempty"`
	ConfigPath string `json:"config_path,omitempty"`
	Status     string `json:"status"`
	Generation uint64 `json:"generation,omitempty"`
}

type ConfigValidationResult struct {
	Valid     bool   `json:"valid"`
	Path      string `json:"path"`
	Version   int    `json:"version"`
	Services  int    `json:"services"`
	Tasks     int    `json:"tasks"`
	Workflows int    `json:"workflows"`
	Schedules int    `json:"schedules"`
}

type DaemonLogChunk struct {
	Data       string `json:"data"`
	NextOffset int64  `json:"next_offset"`
}

type StartupStatus struct {
	Platform    string `json:"platform"`
	Installed   bool   `json:"installed"`
	BootEnabled bool   `json:"boot_enabled"`
	Detail      string `json:"detail,omitempty"`
}

type DoctorReport struct {
	Platform              string                 `json:"platform"`
	Root                  string                 `json:"root"`
	Registry              string                 `json:"registry"`
	Logs                  string                 `json:"logs"`
	DaemonLog             string                 `json:"daemon_log"`
	ExecutionHistory      string                 `json:"execution_history"`
	HistoryDatabase       HistoryDatabaseHealth  `json:"history_database"`
	RegistryOK            bool                   `json:"registry_ok"`
	RegistryError         string                 `json:"registry_error,omitempty"`
	Daemon                map[string]interface{} `json:"daemon"`
	Environment           map[string]string      `json:"environment"`
	Capabilities          CapabilityReport       `json:"capabilities"`
	Startup               *StartupStatus         `json:"startup,omitempty"`
	StartupError          string                 `json:"startup_error,omitempty"`
	DaemonExecutable      string                 `json:"daemon_executable,omitempty"`
	DaemonExecutableError string                 `json:"daemon_executable_error,omitempty"`
}
