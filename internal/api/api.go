// Package api contains the data exchanged between mango clients and mangod.
package api

import (
	"time"

	"github.com/kevin93203/mango/internal/version"
)

const (
	StateStopped    = "stopped"
	StateStarting   = "starting"
	StateWaiting    = "waiting"
	StateRunning    = "running"
	StateStopping   = "stopping"
	StateExited     = "exited"
	StateBackingOff = "backing_off"
	StateCrashLoop  = "crash_loop"
	StateFailed     = "failed"
	StateOrphaned   = "orphaned"
	StateDisabled   = "disabled"
	StateUnknown    = "unknown"
)

const (
	CapabilitySupported   = "supported"
	CapabilityUnsupported = "unsupported"
	CapabilityDegraded    = "degraded"
)

const (
	ProjectPhaseReconciling = "reconciling"
	ProjectPhaseReady       = "ready"
	ProjectPhaseDegraded    = "degraded"
	ProjectPhaseUnavailable = "unavailable"
	ResourcePhasePending    = "pending"
	ResourcePhaseReady      = "ready"
	ResourcePhaseDegraded   = "degraded"
	ResourcePhaseRemoved    = "removed"
)

// ApplyResult reports acceptance of a desired-state generation. Runtime
// convergence is reported separately through ProjectStatus.
type ApplyResult struct {
	Project          string    `json:"project"`
	Generation       uint64    `json:"generation"`
	Status           string    `json:"status"`
	AcceptedAt       time.Time `json:"accepted_at"`
	SourceGeneration uint64    `json:"source_generation,omitempty"`
}

type ReconcileError struct {
	Message    string    `json:"message"`
	At         time.Time `json:"at"`
	Generation uint64    `json:"generation"`
}

type ResourceStatus struct {
	Kind                string `json:"kind"`
	Name                string `json:"name"`
	Phase               string `json:"phase"`
	Pending             bool   `json:"pending"`
	DesiredFingerprint  string `json:"desired_fingerprint,omitempty"`
	ObservedFingerprint string `json:"observed_fingerprint,omitempty"`
	ObservedState       string `json:"observed_state,omitempty"`
	Healthy             *bool  `json:"healthy,omitempty"`
	Error               string `json:"error,omitempty"`
}

type ProjectStatus struct {
	Project    string           `json:"project"`
	Generation uint64           `json:"generation"`
	Phase      string           `json:"phase"`
	Ready      bool             `json:"ready"`
	AcceptedAt time.Time        `json:"accepted_at"`
	LastError  *ReconcileError  `json:"last_error,omitempty"`
	Resources  []ResourceStatus `json:"resources"`
}

// CapabilityInfo describes one host or implementation capability. The state
// is intentionally explicit so callers can distinguish a supported feature
// from a known limitation and from a degraded fallback.
type CapabilityInfo struct {
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

// CapabilityReport is returned by the daemon health API and by mango doctor.
type CapabilityReport struct {
	Platform     string                    `json:"platform"`
	Capabilities map[string]CapabilityInfo `json:"capabilities"`
}

// ServiceInfo is the public representation of a managed service and its
// process tree.
type ServiceInfo struct {
	ID            int
	Project       string
	Name          string
	Supervisor    string
	ProcessName   string
	State         string
	PID           int
	Ports         []string
	StartedAt     time.Time
	UptimeSeconds float64
	CPUPercent    float64
	RSSBytes      uint64
	MemoryPercent float64
	RestartCount  int
	LastExitCode  *int
	LastError     string
	StdoutPath    string
	StderrPath    string
	CommandLine   string
	Disabled      bool
	Health        *HealthInfo
	OSState       string
	WaitingOn     []DependencyStatus    `json:"WaitingOn,omitempty"`
	Children      []ChildProcessInfo    `json:"Children,omitempty"`
	Resources     *ResourceLimitsStatus `json:"resources,omitempty"`
}

type ResourceLimitsStatus struct {
	ProcessLimit CapabilityInfo `json:"process_limit"`
	Memory       CapabilityInfo `json:"memory"`
	CPUPercent   CapabilityInfo `json:"cpu_percent"`
	Overall      string         `json:"overall"`
	Detail       string         `json:"detail,omitempty"`
}

// ServiceOperationResult is the result of one service lifecycle operation.
// Bulk operations return one result for every resolved service or target
// error, allowing the caller to report partial failures without losing the
// successful operations.
type ServiceOperationResult struct {
	Key    string `json:"key"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// ScheduleOperationResult is the result of one schedule enable/disable
// operation. Bulk operations return one result for every resolved schedule or
// target error.
type ScheduleOperationResult struct {
	Key    string `json:"key"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type DependencyStatus struct {
	Service   string `json:"Service"`
	Condition string `json:"Condition"`
	State     string `json:"State"`
	Health    string `json:"Health,omitempty"`
}

type HealthInfo struct {
	Status           string            `json:"Status"`
	Policy           string            `json:"Policy,omitempty"`
	OnUnhealthy      string            `json:"OnUnhealthy,omitempty"`
	Readiness        string            `json:"Readiness,omitempty"`
	Liveness         string            `json:"Liveness,omitempty"`
	Action           string            `json:"Action,omitempty"`
	Checks           []HealthCheckInfo `json:"Checks,omitempty"`
	FailingStreak    int               `json:"FailingStreak,omitempty"`
	LastCheckedAt    *time.Time        `json:"LastCheckedAt,omitempty"`
	LastSuccessAt    *time.Time        `json:"LastSuccessAt,omitempty"`
	LastTransitionAt *time.Time        `json:"LastTransitionAt,omitempty"`
	LastError        string            `json:"LastError,omitempty"`
}

type HealthCheckInfo struct {
	Name          string     `json:"Name"`
	Status        string     `json:"Status"`
	FailingStreak int        `json:"FailingStreak"`
	LastCheckedAt *time.Time `json:"LastCheckedAt,omitempty"`
	LastSuccessAt *time.Time `json:"LastSuccessAt,omitempty"`
	LastError     string     `json:"LastError,omitempty"`
}

// HistoryDatabaseHealth describes the history database connection currently
// used by the daemon. The status is authoritative only when returned by the
// daemon health endpoint.
type HistoryDatabaseHealth struct {
	Driver         string                 `json:"driver"`
	Location       string                 `json:"location"`
	Status         string                 `json:"status"`
	Error          string                 `json:"error,omitempty"`
	LatencyMS      int64                  `json:"latency_ms,omitempty"`
	ConnectionInfo DatabaseConnectionInfo `json:"connection_info"`
	Schema         HistorySchemaHealth    `json:"schema"`
	Migration      HistoryMigrationHealth `json:"migration"`
}

// DatabaseConnectionInfo contains safe, non-secret connection metadata.
// Passwords and arbitrary DSN options are intentionally not represented.
type DatabaseConnectionInfo struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Host     string `json:"host,omitempty"`
	Database string `json:"database,omitempty"`
	Login    string `json:"login,omitempty"`
	Port     int    `json:"port,omitempty"`
	Status   string `json:"status"`
}

// HistorySchemaHealth describes whether the tables required by execution
// history are available to the daemon.
type HistorySchemaHealth struct {
	Status  string   `json:"status"`
	Missing []string `json:"missing,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// HistoryMigrationHealth reports migration state without exposing database
// credentials. Paths are included only for local SQLite databases.
type HistoryMigrationHealth struct {
	Status           string `json:"status"`
	CurrentVersion   int    `json:"current_version"`
	TargetVersion    int    `json:"target_version"`
	MarkerPath       string `json:"marker_path,omitempty"`
	BackupPath       string `json:"backup_path,omitempty"`
	ChecksumMismatch bool   `json:"checksum_mismatch,omitempty"`
	Error            string `json:"error,omitempty"`
	Recovery         string `json:"recovery,omitempty"`
}

// Build is the non-secret build metadata reported by health endpoints.
type Build = version.BuildInfo

type ChildProcessInfo struct {
	PID           int
	ParentPID     int
	Depth         int
	Name          string
	CommandLine   string
	State         *string
	OSState       string
	CPUPercent    float64
	RSSBytes      uint64
	MemoryPercent float64
	Ports         []string
	Children      []ChildProcessInfo `json:"Children,omitempty"`
}

type ServiceListRow struct {
	ParentIndex   int
	Managed       bool
	ID            int
	Project       string
	Service       string
	Supervisor    string
	Process       string
	Name          string
	Depth         int
	PID           int
	Ports         []string
	State         string
	Health        string
	OSState       string
	CPUPercent    float64
	RSSBytes      uint64
	MemoryPercent float64
	RestartCount  int
}

// ProcessListRow is kept as an internal source-compatibility alias. New
// clients should use ServiceListRow.
type ProcessListRow = ServiceListRow

type ScheduleInfo struct {
	Project         string
	Name            string
	Cron            string
	Timezone        string
	TargetType      string
	Target          string
	Misfire         string
	MaxCatchUp      int
	TimeoutSeconds  float64
	Runs            uint64
	Status          string
	Disabled        bool
	LastRun         *time.Time
	NextRun         *time.Time
	DurationSeconds *float64
	LastTrigger     *TriggerInfo
	NextTrigger     *TriggerInfo
}

type TriggerInfo struct {
	Type    string `json:"type"`
	Name    string `json:"name,omitempty"`
	Mode    string `json:"mode,omitempty"`
	EventID string `json:"event_id,omitempty"`
}

// ExecutionInfo is the stable public representation of a logical execution.
// It intentionally contains no process-ownership details; those remain the
// responsibility of mango-shim and the service APIs.
type ExecutionInfo struct {
	RunID                   string       `json:"run_id"`
	Project                 string       `json:"project"`
	Name                    string       `json:"name"`
	TargetType              string       `json:"target_type"`
	Target                  string       `json:"target"`
	Status                  string       `json:"status"`
	Trigger                 *TriggerInfo `json:"trigger,omitempty"`
	IdempotencyKey          string       `json:"idempotency_key,omitempty"`
	ConfigurationGeneration uint64       `json:"configuration_generation,omitempty"`
	RetriedFromRunID        string       `json:"retried_from_run_id,omitempty"`
	CreatedAt               *time.Time   `json:"created_at,omitempty"`
	StartedAt               *time.Time   `json:"started_at,omitempty"`
	FinishedAt              *time.Time   `json:"finished_at,omitempty"`
	ExitCode                int          `json:"exit_code"`
	Error                   string       `json:"error,omitempty"`
	StdoutPath              string       `json:"stdout_path,omitempty"`
	StderrPath              string       `json:"stderr_path,omitempty"`
}

// HistoryInfo is the stable terminal-execution representation returned by
// history.ls. It deliberately exposes operational columns and safe execution
// metadata instead of scheduler's internal structs.
type HistoryInfo struct {
	RunID            string               `json:"run_id"`
	Project          string               `json:"project"`
	Name             string               `json:"name"`
	TargetType       string               `json:"target_type"`
	Target           string               `json:"target"`
	Status           string               `json:"status"`
	Trigger          *TriggerInfo         `json:"trigger,omitempty"`
	RetriedFromRunID string               `json:"retried_from_run_id,omitempty"`
	StartedAt        *time.Time           `json:"started_at,omitempty"`
	FinishedAt       *time.Time           `json:"finished_at,omitempty"`
	ExitCode         int                  `json:"exit_code"`
	Error            string               `json:"error,omitempty"`
	StdoutPath       string               `json:"stdout_path,omitempty"`
	StderrPath       string               `json:"stderr_path,omitempty"`
	Tasks            []HistoryTaskInfo    `json:"tasks,omitempty"`
	Attempts         []HistoryAttemptInfo `json:"attempts,omitempty"`
}

type HistoryTaskInfo struct {
	RunID             string               `json:"run_id"`
	ParentRunID       string               `json:"parent_run_id,omitempty"`
	Node              string               `json:"node,omitempty"`
	Task              string               `json:"task"`
	Command           string               `json:"command,omitempty"`
	Args              []string             `json:"args,omitempty"`
	WorkingDir        string               `json:"working_dir,omitempty"`
	EnvKeys           []string             `json:"env_keys,omitempty"`
	ArgsRedacted      bool                 `json:"args_redacted,omitempty"`
	Status            string               `json:"status"`
	SkipReason        string               `json:"skip_reason,omitempty"`
	TimeoutSeconds    float64              `json:"timeout_seconds,omitempty"`
	RetryCount        int                  `json:"retry_count,omitempty"`
	RetryDelaySeconds float64              `json:"retry_delay_seconds,omitempty"`
	AllowFailure      bool                 `json:"allow_failure,omitempty"`
	PolicyResolved    bool                 `json:"policy_resolved,omitempty"`
	StartedAt         *time.Time           `json:"started_at,omitempty"`
	FinishedAt        *time.Time           `json:"finished_at,omitempty"`
	ElapsedSeconds    float64              `json:"elapsed_seconds,omitempty"`
	ExitCode          int                  `json:"exit_code"`
	Error             string               `json:"error,omitempty"`
	Stderr            string               `json:"stderr,omitempty"`
	StdoutPath        string               `json:"stdout_path,omitempty"`
	StderrPath        string               `json:"stderr_path,omitempty"`
	Attempts          []HistoryAttemptInfo `json:"attempts,omitempty"`
	Artifacts         []ArtifactInfo       `json:"artifacts,omitempty"`
}

type ArtifactInfo struct {
	Path   string `json:"path"`
	Exists bool   `json:"exists"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
}

type HistoryAttemptInfo struct {
	Number         int        `json:"number"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	ElapsedSeconds float64    `json:"elapsed_seconds,omitempty"`
	ExitCode       int        `json:"exit_code"`
	Error          string     `json:"error,omitempty"`
	Stderr         string     `json:"stderr,omitempty"`
}

// HistoryDetail is returned by history.get/show. It includes the complete
// workflow node/task, attempt, and lifecycle event records for the terminal run.
type HistoryDetail struct {
	HistoryInfo
	Events []ExecutionEventInfo `json:"events,omitempty"`
}

type ExecutionEventInfo struct {
	Type      string     `json:"type"`
	Status    string     `json:"status,omitempty"`
	Details   string     `json:"details,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

type ExecutionLogEntry struct {
	Node       string `json:"node,omitempty"`
	Task       string `json:"task,omitempty"`
	Stream     string `json:"stream"`
	Path       string `json:"path"`
	Data       string `json:"data"`
	NextOffset int64  `json:"next_offset,omitempty"`
}

type ExecutionLogs struct {
	RunID  string              `json:"run_id"`
	Stream string              `json:"stream"`
	Logs   []ExecutionLogEntry `json:"logs"`
}

type TaskInfo struct {
	Project         string
	Name            string
	Command         string
	Args            []string
	WorkingDir      string
	TimeoutSeconds  float64
	Concurrency     string
	RetryCount      int
	Runs            uint64
	Status          string
	LastRun         *time.Time
	NextRun         *time.Time
	DurationSeconds *float64
	LastTrigger     *TriggerInfo
	NextTrigger     *TriggerInfo
}

type WorkflowTaskInfo struct {
	Node            string
	Uses            string
	Needs           []string
	Status          string
	LastRun         *time.Time
	DurationSeconds *float64
}

type WorkflowInfo struct {
	Project         string
	Name            string
	Concurrency     string
	Status          string
	TaskCount       int
	Runs            uint64
	LastRun         *time.Time
	NextRun         *time.Time
	DurationSeconds *float64
	LastTrigger     *TriggerInfo
	NextTrigger     *TriggerInfo
	Tasks           []WorkflowTaskInfo
}

func HealthDisplay(info *HealthInfo) string {
	if info == nil {
		return ""
	}
	return info.Status
}

func FlattenServiceList(items []ServiceInfo) []ServiceListRow {
	rows := make([]ServiceListRow, 0, len(items))
	for parentIndex, item := range items {
		rows = append(rows, ServiceListRow{
			ParentIndex:   parentIndex,
			Managed:       true,
			ID:            item.ID,
			Project:       item.Project,
			Service:       item.Project + "/" + item.Name,
			Supervisor:    item.Supervisor,
			Process:       item.ProcessName,
			Name:          item.Name,
			Depth:         0,
			PID:           item.PID,
			Ports:         item.Ports,
			State:         item.State,
			Health:        HealthDisplay(item.Health),
			OSState:       item.OSState,
			CPUPercent:    item.CPUPercent,
			RSSBytes:      item.RSSBytes,
			MemoryPercent: item.MemoryPercent,
			RestartCount:  item.RestartCount,
		})
		appendChildRows(&rows, parentIndex, item.Project, item.Children)
	}
	return rows
}

func appendChildRows(rows *[]ServiceListRow, parentIndex int, project string, children []ChildProcessInfo) {
	for _, child := range children {
		*rows = append(*rows, ServiceListRow{
			ParentIndex:   parentIndex,
			Project:       project,
			Process:       child.Name,
			Name:          child.Name,
			Depth:         child.Depth,
			PID:           child.PID,
			Ports:         child.Ports,
			OSState:       child.OSState,
			CPUPercent:    child.CPUPercent,
			RSSBytes:      child.RSSBytes,
			MemoryPercent: child.MemoryPercent,
		})
		appendChildRows(rows, parentIndex, project, child.Children)
	}
}
