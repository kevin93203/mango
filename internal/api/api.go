// Package api contains the data exchanged between mango clients and mangod.
package api

import "time"

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
	StateDisabled   = "disabled"
	StateUnknown    = "unknown"
)

// ServiceInfo is the public representation of a managed service and its
// process tree.
type ServiceInfo struct {
	ID            int
	Project       string
	Name          string
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
	WaitingOn     []DependencyStatus `json:"WaitingOn,omitempty"`
	Children      []ChildProcessInfo `json:"Children,omitempty"`
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

type DependencyStatus struct {
	Service   string `json:"Service"`
	Condition string `json:"Condition"`
	State     string `json:"State"`
	Health    string `json:"Health,omitempty"`
}

type HealthInfo struct {
	Status        string            `json:"Status"`
	Policy        string            `json:"Policy,omitempty"`
	Checks        []HealthCheckInfo `json:"Checks,omitempty"`
	FailingStreak int               `json:"FailingStreak,omitempty"`
	LastCheckedAt *time.Time        `json:"LastCheckedAt,omitempty"`
	LastSuccessAt *time.Time        `json:"LastSuccessAt,omitempty"`
	LastError     string            `json:"LastError,omitempty"`
}

type HealthCheckInfo struct {
	Name          string     `json:"Name"`
	Status        string     `json:"Status"`
	FailingStreak int        `json:"FailingStreak"`
	LastCheckedAt *time.Time `json:"LastCheckedAt,omitempty"`
	LastSuccessAt *time.Time `json:"LastSuccessAt,omitempty"`
	LastError     string     `json:"LastError,omitempty"`
}

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
	Action          string
	Target          string
	Concurrency     string
	TimeoutSeconds  float64
	Status          string
	LastRun         *time.Time
	NextRun         *time.Time
	DurationSeconds *float64
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
